import shutil
import csv
import os
import ssl
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import atexit
from time import sleep
import sys
from datetime import datetime
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Logger:
    def __init__(self, logfile):
        self.terminal = sys.stdout
        # line-buffered file (best for "live" tailing)
        self.log = open(logfile, "a", buffering=1, encoding="utf-8", errors="replace")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        # ✅ force live output
        self.terminal.flush()
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# vCenter Functions
def get_obj(content, vimtype, name):
    container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
    for c in container.view:
        if c.name == name:
            return c
    return None

def wait_for_task(task):
    while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
        sleep(2)
    if task.info.state == vim.TaskInfo.State.error:
        raise Exception(task.info.error.msg)

def cidr_to_netmask(cidr):
    cidr = int(cidr)
    mask = (0xffffffff >> (32 - cidr)) << (32 - cidr)
    return ".".join([str((mask >> (i * 8)) & 0xff) for i in range(4)[::-1]])

def reconfigure_vm(vm, cpu, ram, disk):
    spec = vim.vm.ConfigSpec()
    spec.numCPUs = int(cpu)
    spec.memoryMB = int(ram) * 1024
    for dev in vm.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualDisk):
            disk_spec = vim.vm.device.VirtualDeviceSpec()
            disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
            disk_spec.device = dev
            disk_spec.device.capacityInKB = int(disk) * 1024 * 1024
            spec.deviceChange = [disk_spec]
            break
    task = vm.ReconfigVM_Task(spec=spec)
    wait_for_task(task)

def add_disk_to_vm(vm, disk_size_gb, unit_number=None):
    spec = vim.vm.ConfigSpec()
    new_disk_kb = int(disk_size_gb) * 1024 * 1024
    controller = None
    for dev in vm.config.hardware.device:
        if isinstance(dev, (vim.vm.device.VirtualSCSIController, vim.vm.device.ParaVirtualSCSIController)):
            controller = dev
            break
    if not controller:
        raise Exception("No SCSI controller found to attach new disk.")
    used_unit_numbers = [dev.unitNumber for dev in vm.config.hardware.device if hasattr(dev, 'unitNumber')]
    if unit_number is None:
        next_unit = 0
        while next_unit in used_unit_numbers or next_unit == 7:
            next_unit += 1
            if next_unit > 15:
                raise Exception("No free unit number available on SCSI controller")
    else:
        next_unit = unit_number
    disk_spec = vim.vm.device.VirtualDeviceSpec()
    disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    disk_spec.fileOperation = vim.vm.device.VirtualDeviceSpec.FileOperation.create
    disk = vim.vm.device.VirtualDisk()
    disk.capacityInKB = new_disk_kb
    disk.unitNumber = next_unit
    disk.controllerKey = controller.key
    disk.key = -100
    backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
    backing.diskMode = 'persistent'
    backing.thinProvisioned = False
    backing.fileName = ""
    disk.backing = backing
    disk_spec.device = disk
    spec.deviceChange = [disk_spec]
    task = vm.ReconfigVM_Task(spec=spec)
    wait_for_task(task)

def connect_vm_nics(vm):
    spec = vim.vm.ConfigSpec()
    device_changes = []
    for dev in vm.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualEthernetCard):
            dev_spec = vim.vm.device.VirtualDeviceSpec()
            dev_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
            dev.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
            dev.connectable.connected = True
            dev.connectable.startConnected = True
            dev.connectable.allowGuestControl = True
            dev_spec.device = dev
            device_changes.append(dev_spec)
    spec.deviceChange = device_changes
    if device_changes:
        task = vm.ReconfigVM_Task(spec=spec)
        wait_for_task(task)

# 🆕 Function to update Notes field in vCenter
def update_vm_notes(vm, notes_text):
    try:
        spec = vim.vm.ConfigSpec()
        spec.annotation = notes_text
        task = vm.ReconfigVM_Task(spec=spec)
        wait_for_task(task)
    except Exception as e:
        print(f"❌ Failed to update notes for {vm.name}: {e}")

def deploy_vms_from_csv(vcenter, username, password, csv_file_path, logger):
    context = ssl._create_unverified_context()
    si = SmartConnect(host=vcenter, user=username, pwd=password, sslContext=context)
    atexit.register(Disconnect, si)
    content = si.RetrieveContent()
    successful_vms = []

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row = {k: v.strip() if v else '' for k, v in row.items()}
            name = row['Name']
            source_vm = get_obj(content, [vim.VirtualMachine], row['Source_vm'])
            if not source_vm:
                logger.write(f"❌ Source VM {row['Source_vm']} not found.\n")
                continue

            folder = source_vm.parent
            resource_pool = get_obj(content, [vim.ResourcePool], row['ResourcePool']) if row.get('ResourcePool') else source_vm.resourcePool
            if not resource_pool:
                logger.write(f"⚠️ Resource pool '{row['ResourcePool']}' not found. Using source VM's pool.\n")
                resource_pool = source_vm.resourcePool
            datastore = source_vm.datastore[0]

            relospec = vim.vm.RelocateSpec()
            relospec.datastore = datastore
            relospec.pool = resource_pool
            clonespec = vim.vm.CloneSpec()
            clonespec.location = relospec
            clonespec.powerOn = False

            adapter1 = vim.vm.customization.AdapterMapping()
            adapter1.adapter = vim.vm.customization.IPSettings()
            adapter1.adapter.ip = vim.vm.customization.FixedIp()
            adapter1.adapter.ip.ipAddress = row['Nic1_ip']
            adapter1.adapter.subnetMask = cidr_to_netmask(row['Nic1_subnet'])
            adapter1.adapter.gateway = [row['Gateway']]

            nic_settings = [adapter1]

            if row.get('Nic2_ip') and row.get('Nic2_subnet') and row.get('Nic2_network'):
                adapter2 = vim.vm.customization.AdapterMapping()
                adapter2.adapter = vim.vm.customization.IPSettings()
                adapter2.adapter.ip = vim.vm.customization.FixedIp()
                adapter2.adapter.ip.ipAddress = row['Nic2_ip']
                adapter2.adapter.subnetMask = cidr_to_netmask(row['Nic2_subnet'])
                nic_settings.append(adapter2)

            globalip = vim.vm.customization.GlobalIPSettings()
            globalip.dnsServerList = row['DNS'].split(',')

            ident = vim.vm.customization.LinuxPrep()
            ident.hostName = vim.vm.customization.FixedName()
            csv_hostname = row.get('hostname', '').strip()
            if csv_hostname:
                ident.hostName.name = csv_hostname
            else:
                ident.hostName.name = name
            ident.domain = "local"

            customspec = vim.vm.customization.Specification()
            customspec.nicSettingMap = nic_settings
            customspec.globalIPSettings = globalip
            customspec.identity = ident
            clonespec.customization = customspec

            try:
                logger.write(f"🔁 Cloning {name} from {row['Source_vm']}\n")
                task = source_vm.Clone(folder=folder, name=name, spec=clonespec)
                wait_for_task(task)

                new_vm = get_obj(content, [vim.VirtualMachine], name)
                if not new_vm:
                    logger.write(f"❌ VM {name} not found after cloning.\n")
                    continue

                reconfigure_vm(new_vm, row['CPU'], row['RAM'], row['Disk'])
                connect_vm_nics(new_vm)

                # 🆕 Update VM Notes field with IPs, Hostname, Gateway
                vm_notes = ""
                if row.get('Nic1_ip'):
                    vm_notes += f"{row['Nic1_ip']}\n"
                if row.get('Nic2_ip'):
                    vm_notes += f"{row['Nic2_ip']}\n"
#                if row.get('hostname'):
#                    vm_notes += f"Hostname: {row['hostname']}\n"
#                if row.get('Gateway'):
#                    vm_notes += f"Gateway: {row['Gateway']}"
                update_vm_notes(new_vm, vm_notes.strip())

                power_task = new_vm.PowerOnVM_Task()
                wait_for_task(power_task)

                successful_vms.append(row)

                additional_disk = row.get('Additional_Disk', '').strip()
                if additional_disk:
                    try:
                        disk_size = int(additional_disk)
                        # Find all virtual disks
                        disks = [dev for dev in new_vm.config.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]

                        if len(disks) >= 2:
                            # Extend the second disk
                            second_disk = disks[1]
                            logger.write(f"🧩 Extending existing second disk for {name} to {disk_size} GB\n")

                            disk_spec = vim.vm.device.VirtualDeviceSpec()
                            disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                            disk_spec.device = second_disk
                            disk_spec.device.capacityInKB = disk_size * 1024 * 1024  # GB → KB

                            spec = vim.vm.ConfigSpec()
                            spec.deviceChange = [disk_spec]
                            task = new_vm.ReconfigVM_Task(spec=spec)
                            wait_for_task(task)
                            logger.write(f"✅ Extended second disk to {disk_size} GB for {name}\n")

                        else:
                            # Only one disk — add a new one
                            logger.write(f"➕ Only one disk found, adding new additional disk of {disk_size} GB to {name}\n")
                            add_disk_to_vm(new_vm, disk_size)

                    except Exception as e:
                        logger.write(f"❌ Failed to handle additional disk for {name}: {e}\n")
                logger.write(f"✅ {name} deployed successfully.\n\n")
            except Exception as e:
                logger.write(f"❌ Error deploying {name}: {e}\n")
                continue

    return successful_vms

def main():
    load_dotenv()
    open("logs.txt", "w").close()
    logger = Logger("logs.txt")
    sys.stdout = logger
    sys.stderr = logger

    folder_name = None
    try:
        if len(sys.argv) < 2:
            logger.write("Usage: python3 deploy_vms.py <csv_file>\n")
            sys.exit(1)

        csv_file_path = sys.argv[1]
        vcenter = os.getenv("VCENTER_HOST")
        username = os.getenv("VCENTER_USER")
        password = os.getenv("VCENTER_PASS")

        if not all([vcenter, username, password]):
            logger.write("❌ Missing credentials in .env file (VCENTER_HOST, VCENTER_USER, VCENTER_PASS)\n")
            sys.exit(1)

        logger.write("🚀 Starting VM deployment...\n")
        successful_vms = deploy_vms_from_csv(vcenter, username, password, csv_file_path, logger)
        logger.write("✅ VM deployment completed.\n\n")

        now_dt = datetime.now()
        date_str = now_dt.strftime("%d-%m-%Y")
        time_str = now_dt.strftime("%H:%M")
        folder_name = f"./results/{date_str}_{time_str}"
        os.makedirs(folder_name, exist_ok=True)

        new_csv_name = f"Deploy_csv_{date_str}.csv"
        new_csv_path = os.path.join(folder_name, new_csv_name)
        shutil.copy(csv_file_path, new_csv_path)

        logger.write(f"✅ Deployment completed. Results folder: {folder_name}\n")

    except Exception as e:
        import traceback
        logger.write(f"❌ An unexpected error occurred: {e}\n")
        traceback.print_exc()
    finally:
        logger.log.flush()
        logger.log.close()
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        if folder_name is None:
            now_dt = datetime.now()
            date_str = now_dt.strftime("%d-%m-%Y")
            time_str = now_dt.strftime("%H:%M")
            folder_name = f"./results/{date_str}_{time_str}"
            os.makedirs(folder_name, exist_ok=True)

        new_log_name = f"Deploy_log_{date_str}.txt"
        new_log_path = os.path.join(folder_name, new_log_name)
        shutil.copy("logs.txt", new_log_path)

        print(f"📝 Logs file copied to: {folder_name}")

if __name__ == "__main__":
    main()
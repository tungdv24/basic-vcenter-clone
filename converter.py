import csv
import ipaddress

class IPRangeMatchError(ValueError):
    """Raised when an IP address does not match any subnet in ip_ranges.csv."""
    pass


def convert_simple_csv_to_deploy_csv(source_file, ip_ranges_file, output_file):
    ip_ranges = []
    with open(ip_ranges_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ip_ranges.append({
                "subnet": ipaddress.ip_network(row["subnet"].strip()),
                "network": row["network"].strip(),
                "gateway": row["gateway"].strip()
            })

    if not ip_ranges:
        raise ValueError("ip_ranges.csv is empty (no subnets loaded)")

    def find_network_info_strict(ip: str, *, row_index: int, field_name: str):
        """
        Returns (network, subnet_prefix, gateway) for IP.
        Raises IPRangeMatchError if IP is present but does not match any subnet.
        Raises ValueError if IP is invalid format.
        """
        ip = (ip or "").strip()
        if not ip:
            return "", "", ""

        try:
            ip_addr = ipaddress.ip_address(ip)
        except ValueError:
            raise ValueError(f"Row {row_index}: invalid {field_name} '{ip}'")

        for item in ip_ranges:
            if ip_addr in item["subnet"]:
                return item["network"], item["subnet"].prefixlen, item["gateway"]

        raise IPRangeMatchError(
            f"Row {row_index}: {field_name} '{ip}' does not match any subnet in ip_ranges.csv"
        )

    with open(source_file, newline="") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.DictReader(infile, delimiter=",")

        fieldnames = [
            "Name", "Source_vm", "CPU", "RAM", "Disk",
            "Nic1_network", "Nic1_ip", "Nic1_subnet",
            "Nic2_network", "Nic2_ip", "Nic2_subnet",
            "Gateway", "DNS", "Additional_Disk", "ResourcePool", "hostname"
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for idx, row in enumerate(reader, start=2):  # start=2 because row 1 is header
            ip1 = (row.get("data.IP1", "") or "").strip()
            ip2 = (row.get("data.IP2", "") or "").strip()

            # If IP1 missing but IP2 exists: swap so NIC1 is always primary
            if not ip1 and ip2:
                ip1, ip2 = ip2, ""

            # NIC1 (strict if ip1 provided)
            nic1_network, nic1_subnet, gateway = find_network_info_strict(
                ip1, row_index=idx, field_name="data.IP1"
            ) if ip1 else ("", "", "")

            # NIC2 (strict if ip2 provided)
            if ip2:
                nic2_network, nic2_subnet, _ = find_network_info_strict(
                    ip2, row_index=idx, field_name="data.IP2"
                )
            else:
                nic2_network, nic2_subnet = "", ""

            writer.writerow({
                "Name": (row.get("data.VMName", "") or "").strip(),
                "Source_vm": (row.get("data.OSTemplate", "") or "").strip(),
                "CPU": (row.get("data.CPU", "") or "").strip(),
                "RAM": (row.get("data.RAM", "") or "").strip(),
                "Disk": (row.get("data.Disk", "") or "").strip(),
                "Nic1_network": nic1_network,
                "Nic1_ip": ip1,
                "Nic1_subnet": nic1_subnet,
                "Nic2_network": nic2_network,
                "Nic2_ip": ip2,
                "Nic2_subnet": nic2_subnet,
                "Gateway": gateway,
                "DNS": "8.8.8.8,208.67.220.220",
                "Additional_Disk": (row.get("data.AdditionalDisk", "") or "").strip(),
                "ResourcePool": (row.get("data.ResourcePool", "") or "").strip(),
                "hostname": (row.get("data.hostname", "") or "").strip()
            })
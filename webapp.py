# webapp.py
import os
import csv
import uuid
import threading
from datetime import datetime
import ssl
import atexit
from datetime import timedelta


from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    abort,
    Response,
    send_file,
    flash
)

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

from converter import convert_simple_csv_to_deploy_csv
from vc_deploy import deploy_vms_from_csv, Logger  # rename your old app.py -> vc_deploy.py


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "CHANGE_ME_PLEASE")
# app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=1)

app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=15)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Hard-coded backend ip_ranges.csv path
IP_RANGES_PATH = os.path.join(DATA_DIR, "ip_ranges.csv")

# In-memory job registry
# jobs[job_id] = {
#   "log_path": str,
#   "status": "staged"|"running"|"done"|"error",
#   "error": str,
#   "results_dir": str,
#   "converted_csv_path": str,
#   "simple_csv_path": str,
#   "validation_status": "pending"|"pass"|"fail",
#   "validation_errors": list[str],
# }
jobs = {}
jobs_lock = threading.Lock()


def require_login() -> bool:
    return all(session.get(k) for k in ("vcenter_host", "vcenter_user", "vcenter_pass"))


def write_simple_csv(simple_csv_path: str, form_data) -> None:
    """
    Create a simple.csv (convert.py input format) from the single-VM form.
    """
    headers = [
        "data.VMName",
        "data.OSTemplate",
        "data.CPU",
        "data.RAM",
        "data.Disk",
        "data.IP1",
        "data.IP2",
        "data.AdditionalDisk",
        "data.ResourcePool",
        "data.hostname",
    ]

    with open(simple_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerow(
            {
                "data.VMName": form_data.get("data.VMName", "").strip(),
                "data.OSTemplate": form_data.get("data.OSTemplate", "").strip(),
                "data.CPU": form_data.get("data.CPU", "").strip(),
                "data.RAM": form_data.get("data.RAM", "").strip(),
                "data.Disk": form_data.get("data.Disk", "").strip(),
                "data.IP1": form_data.get("data.IP1", "").strip(),
                "data.IP2": form_data.get("data.IP2", "").strip(),
                "data.AdditionalDisk": form_data.get("data.AdditionalDisk", "").strip(),
                "data.ResourcePool": form_data.get("data.ResourcePool", "").strip(),
                "data.hostname": form_data.get("data.hostname", "").strip(),
            }
        )


def validate_simple_csv_headers(path: str) -> None:
    required = {
        "data.VMName",
        "data.OSTemplate",
        "data.CPU",
        "data.RAM",
        "data.Disk",
        "data.IP1",
        "data.IP2",
        "data.AdditionalDisk",
        "data.ResourcePool",
        "data.hostname",
    }

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV missing header row")
        missing = required - set([h.strip() for h in reader.fieldnames])
        if missing:
            raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")


# ---------- vCenter validation (used in staging) ----------

def vc_connect(host: str, user: str, pwd: str):
    ctx = ssl._create_unverified_context()
    si = SmartConnect(host=host, user=user, pwd=pwd, sslContext=ctx)
    atexit.register(Disconnect, si)
    return si


def vc_get_by_name(content, vimtypes, name: str):
    if not name:
        return None
    view = content.viewManager.CreateContainerView(content.rootFolder, vimtypes, True)
    try:
        for obj in view.view:
            if obj.name == name:
                return obj
    finally:
        try:
            view.Destroy()
        except Exception:
            pass
    return None


def validate_converted_with_vcenter(
    vcenter_host: str,
    vcenter_user: str,
    vcenter_pass: str,
    converted_csv_path: str
):
    """
    Validate every row in converted.csv:
      - Source_vm exists (vim.VirtualMachine)
      - Nic1_network exists (vim.Network or vim.dvs.DistributedVirtualPortgroup)
      - Nic2_network exists if provided
      - ResourcePool exists (vim.ResourcePool) if provided

    Returns:
      (ok, errors, source_vm_ok, net_ok, pool_ok)
    """
    si = vc_connect(vcenter_host, vcenter_user, vcenter_pass)
    content = si.RetrieveContent()

    cache_vm = {}
    cache_net = {}
    cache_pool = {}

    source_vm_ok = {}  # name -> bool
    net_ok = {}        # name -> bool
    pool_ok = {}       # name -> bool

    def vm_exists(name: str) -> bool:
        name = (name or "").strip()
        if not name:
            return False
        if name in cache_vm:
            return cache_vm[name]
        obj = vc_get_by_name(content, [vim.VirtualMachine], name)
        cache_vm[name] = (obj is not None)
        return cache_vm[name]

    def net_exists(name: str) -> bool:
        name = (name or "").strip()
        if not name:
            return False
        if name in cache_net:
            return cache_net[name]
        obj = vc_get_by_name(content, [vim.Network, vim.dvs.DistributedVirtualPortgroup], name)
        cache_net[name] = (obj is not None)
        return cache_net[name]

    def pool_exists(name: str) -> bool:
        name = (name or "").strip()
        if not name:
            return True  # empty is allowed (your deploy uses source VM pool)
        if name in cache_pool:
            return cache_pool[name]
        obj = vc_get_by_name(content, [vim.ResourcePool], name)
        cache_pool[name] = (obj is not None)
        return cache_pool[name]

    errors = []

    with open(converted_csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):
            vmname = (row.get("Name") or "").strip()
            src = (row.get("Source_vm") or "").strip()
            n1 = (row.get("Nic1_network") or "").strip()
            n2 = (row.get("Nic2_network") or "").strip()
            rp = (row.get("ResourcePool") or "").strip()

            if src:
                ok_src = vm_exists(src)
                source_vm_ok[src] = ok_src
                if not ok_src:
                    errors.append(f"Row {row_num} (Name={vmname}): Source_vm '{src}' NOT found in vCenter")

            if n1:
                ok_n1 = net_exists(n1)
                net_ok[n1] = ok_n1
                if not ok_n1:
                    errors.append(f"Row {row_num} (Name={vmname}): Nic1_network '{n1}' NOT found in vCenter")

            if n2:
                ok_n2 = net_exists(n2)
                net_ok[n2] = ok_n2
                if not ok_n2:
                    errors.append(f"Row {row_num} (Name={vmname}): Nic2_network '{n2}' NOT found in vCenter")

            # ResourcePool is optional but if provided must exist
            if rp:
                ok_rp = pool_exists(rp)
                pool_ok[rp] = ok_rp
                if not ok_rp:
                    errors.append(f"Row {row_num} (Name={vmname}): ResourcePool '{rp}' NOT found in vCenter")

    ok = (len(errors) == 0)
    return ok, errors, source_vm_ok, net_ok, pool_ok


# ---------- background job runner ----------

def run_job(
    job_id: str,
    vcenter_host: str,
    vcenter_user: str,
    vcenter_pass: str,
    converted_csv_path: str,
    job_results_dir: str,
    log_path: str,
) -> None:
    """
    Background thread function.
    IMPORTANT: do NOT use Flask session/request globals here.
    """
    try:
        logger = Logger(log_path)

        import sys
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout = logger
        sys.stderr = logger
        try:
            logger.write("🚀 Starting VM deployment...\n")
            _successful = deploy_vms_from_csv(
                vcenter_host, vcenter_user, vcenter_pass, converted_csv_path, logger
            )
            logger.write("✅ VM deployment completed.\n")
        finally:
            logger.log.flush()
            logger.log.close()
            sys.stdout = old_out
            sys.stderr = old_err

        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "done"

    except Exception as e:
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = str(e)


# ---------- routes ----------

@app.route("/", methods=["GET"])
def root():
    if require_login():
        return redirect(url_for("index"))
    return redirect(url_for("login"))

@app.before_request
def enforce_session_timeout():
    # skip these endpoints
    if request.endpoint in ("login", "static"):
        return

    # If not logged in, let existing logic handle redirect
    if not require_login():
        return

    now = datetime.utcnow()
    last = session.get("last_activity")

    if last:
        try:
            last_dt = datetime.fromisoformat(last)
        except Exception:
            # bad value -> force logout
            session.clear()
            return redirect(url_for("login"))
        if (now - last_dt) > timedelta(hours=1):
            # inactive too long -> logout
            session.clear()
            return redirect(url_for("login"))

    # update activity timestamp
    session["last_activity"] = now.isoformat()
    session.permanent = True

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", error=None)

    host = request.form.get("host", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")

    if not (host and username and password):
        return render_template("login.html", error="Missing credentials")

    session["vcenter_host"] = host
    session["vcenter_user"] = username
    session["vcenter_pass"] = password
    session["username"] = username

    # ✅ add these
    session["last_activity"] = datetime.utcnow().isoformat()
    session.permanent = True

    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/index", methods=["GET"])
def index():
    if not require_login():
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/deploy", methods=["POST"])
def deploy():
    """
    Convert only → redirect to staging page.
    Supports:
      - Upload pre-configured simple.csv (multi-VM)
      - Or manual form (single VM) -> create simple.csv
    Uses backend data/ip_ranges.csv
    """
    if not require_login():
        return redirect(url_for("login"))

    if not os.path.exists(IP_RANGES_PATH):
        return "Backend ip_ranges.csv missing: data/ip_ranges.csv", 500

    job_id = str(uuid.uuid4())[:8]
    job_upload_dir = os.path.join(UPLOAD_DIR, job_id)
    os.makedirs(job_upload_dir, exist_ok=True)

    now_dt = datetime.now()
    date_str = now_dt.strftime("%d-%m-%Y")
    time_str = now_dt.strftime("%H-%M-%S")
    job_results_dir = os.path.join(RESULTS_DIR, f"{date_str}_{time_str}_{job_id}")
    os.makedirs(job_results_dir, exist_ok=True)

    simple_csv_path = os.path.join(job_upload_dir, "simple.csv")
    converted_csv_path = os.path.join(job_results_dir, "converted.csv")
    log_path = os.path.join(job_results_dir, "logs.txt")

    # If user uploaded pre-configured CSV, use it
    uploaded = request.files.get("simplecsv")
    if uploaded and uploaded.filename:
        if not uploaded.filename.lower().endswith(".csv"):
            return "Uploaded file must be a .csv", 400

        uploaded.save(simple_csv_path)

        try:
            validate_simple_csv_headers(simple_csv_path)
        except Exception as e:
            return f"Invalid uploaded CSV: {e}", 400
    else:
        # Otherwise build single-VM simple.csv from form
        write_simple_csv(simple_csv_path, request.form)

    # Convert using backend ip_ranges.csv
    try:
        convert_simple_csv_to_deploy_csv(simple_csv_path, IP_RANGES_PATH, converted_csv_path)
    except Exception as e:
        return f"Convert failed: {e}", 500

    with jobs_lock:
        jobs[job_id] = {
            "log_path": log_path,
            "status": "staged",
            "error": "",
            "results_dir": job_results_dir,
            "converted_csv_path": converted_csv_path,
            "simple_csv_path": simple_csv_path,
            "validation_status": "pending",
            "validation_errors": [],
        }

    return redirect(url_for("staging", job_id=job_id))


@app.route("/staging/<job_id>", methods=["GET"])
def staging(job_id):
    """
    Display staging table + validate Source_vm / NIC networks / ResourcePool against vCenter.
    Confirm button must only work when validation passes.
    """
    if not require_login():
        return redirect(url_for("login"))

    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        abort(404)

    converted_csv_path = job.get("converted_csv_path")
    if not converted_csv_path or not os.path.exists(converted_csv_path):
        return "converted.csv not found for this job", 404

    # Read converted.csv for table display
    rows = []
    with open(converted_csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        for r in reader:
            rows.append(r)

    # Run validation once (pending -> pass/fail)
    with jobs_lock:
        v_status = job.get("validation_status", "pending")

    if v_status == "pending":
        vcenter_host = session.get("vcenter_host")
        vcenter_user = session.get("vcenter_user")
        vcenter_pass = session.get("vcenter_pass")

        try:
            # ✅ UPDATED: now returns 5 values (includes ResourcePool map)
            ok, errs, src_map, net_map, pool_map = validate_converted_with_vcenter(
                vcenter_host, vcenter_user, vcenter_pass, converted_csv_path
            )
        except Exception as e:
            ok = False
            errs = [f"Validation error: {e}"]
            src_map = {}
            net_map = {}
            pool_map = {}

        with jobs_lock:
            jobs[job_id]["validation_status"] = "pass" if ok else "fail"
            jobs[job_id]["validation_errors"] = errs[:200]
            jobs[job_id]["validation_maps"] = {
                "source_vm_ok": src_map,
                "net_ok": net_map,
                "pool_ok": pool_map,
            }

    with jobs_lock:
        v_status = jobs[job_id]["validation_status"]
        v_errors = jobs[job_id]["validation_errors"]
        v_maps = jobs[job_id].get(
            "validation_maps",
            {"source_vm_ok": {}, "net_ok": {}, "pool_ok": {}}
        )

    return render_template(
        "staging.html",
        job_id=job_id,
        columns=columns,
        rows=rows,
        download_url=url_for("download_converted", job_id=job_id),
        confirm_url=url_for("confirm_deploy", job_id=job_id),
        validation_status=v_status,
        validation_errors=v_errors,
        vc_checks=v_maps,  # ✅ pass maps for ✓/✗
        error=None,
    )

@app.route("/confirm/<job_id>", methods=["POST"])
def confirm_deploy(job_id):
    """
    Start background deploy after user confirms staging.
    Only allowed if vCenter validation passed.
    """
    if not require_login():
        return redirect(url_for("login"))

    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        abort(404)

    # Block confirm if validation not passed
    if job.get("validation_status") != "pass":
        return "Validation not passed. Fix Source_vm / network issues in staging before deploying.", 400

    # If already running/done, just go to clone page
    if job.get("status") in ("running", "done"):
        return redirect(url_for("clone_page", job_id=job_id))

    converted_csv_path = job.get("converted_csv_path")
    log_path = job.get("log_path")
    job_results_dir = job.get("results_dir")

    if not converted_csv_path or not os.path.exists(converted_csv_path):
        return "converted.csv missing, cannot deploy", 400

    # Capture creds inside request context
    vcenter_host = session.get("vcenter_host")
    vcenter_user = session.get("vcenter_user")
    vcenter_pass = session.get("vcenter_pass")

    with jobs_lock:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["error"] = ""

    t = threading.Thread(
        target=run_job,
        args=(
            job_id,
            vcenter_host,
            vcenter_user,
            vcenter_pass,
            converted_csv_path,
            job_results_dir,
            log_path,
        ),
        daemon=True,
    )
    t.start()

    return redirect(url_for("clone_page", job_id=job_id))


@app.route("/clone/<job_id>", methods=["GET"])
def clone_page(job_id):
    if not require_login():
        return redirect(url_for("login"))

    with jobs_lock:
        if job_id not in jobs:
            abort(404)

    return render_template("clone-page.html", job_id=job_id)


@app.route("/logs/<job_id>", methods=["GET"])
def logs(job_id):
    if not require_login():
        return Response("Not logged in", status=401, mimetype="text/plain")

    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return Response("Job not found", status=404, mimetype="text/plain")

    log_path = job["log_path"]
    status = job["status"]
    err = job.get("error", "")

    text = ""
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
        except Exception as e:
            text = f"(Could not read log: {e})\n"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    footer = f"\n\n---\nLAST_UPDATE: {now}\nSTATUS: {status}\n"
    if err:
        footer += f"ERROR: {err}\n"

    return Response(text + footer, mimetype="text/plain")


@app.route("/download/<job_id>/converted.csv", methods=["GET"])
def download_converted(job_id):
    if not require_login():
        return redirect(url_for("login"))

    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        abort(404)

    path = job.get("converted_csv_path")
    if not path or not os.path.exists(path):
        abort(404)

    return send_file(path, as_attachment=True, download_name="converted.csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=55000, debug=True)
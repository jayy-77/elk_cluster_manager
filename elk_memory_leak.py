import yaml, paramiko, itertools, time

file_size_units = {"kb": 1, "mb": 2, "gb": 3, "tb": 4}

with open("inventory.yml") as stream:
    try: inventory = yaml.safe_load(stream)
    except yaml.YAMLError as exc: print(exc)

def run_command(command, host, ssh_user, private_key_path):
    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=ssh_user, key_filename=private_key_path)

    stdin, stdout, stderr = ssh.exec_command(command)
    output = stdout.read().decode().strip()

    ssh.close()

    return output

def manage_log_file(servers):
    restart_status = False

    for server in servers:
        command = f'stat -c %s {inventory["file"]["path"]}'
        size = int(run_command(command, **server)) / (
            1024 ** file_size_units[inventory["file"]["unit"]]
        )
        size = float(f"{size:.3f}")

        if size >= inventory["file"]["threshold"]:
            command = f"rm -f {inventory["file"]["path"]}"
            restart_status = True

    return restart_status

def start_elk(servers):
    health_threshold = 100 / len(servers)

    for server in servers:
        command_start = "service elasticsearch start"
        command_health = f'curl -X GET "http://{server["host"]}:9200/_cluster/health?pretty"'

        run_command(command_start, *server)

        while True:
            health = run_command(command_health, *server)
            if health >= health_threshold: break
            else: time.sleep(2)

def stop_elk(servers):
    command = "service elasticsearch stop"
    for server in servers[::-1]: 
        while True:
            status = run_command(command, *server)
            if status == "inactive": break
            else: time.sleep(2)

def manage_elk_cluster(region):
    servers = list(itertools.chain(*inventory["hosts"][region].values()))
    
    if manage_log_file(servers):
        stop_elk(servers)
        start_elk(servers)

for region in inventory["hosts"].keys():
    manage_elk_cluster(region)

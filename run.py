import subprocess

brokers = subprocess.run(["docker-compose", "up", "-d"])
if brokers.returncode != 0:
    raise Exception("Не удалось запустить брокеров!")


processes = {}
processes["1"] = subprocess.Popen(["c:/Users/User/anaconda3/python", "./scripts/generation.py"])
processes["2"] = subprocess.Popen(["c:/Users/User/anaconda3/python", "./scripts/processing.py"])
processes["3"] = subprocess.Popen(["c:/Users/User/anaconda3/python", "./scripts/ML_inference.py"])
try:
    while True:
        continue
except KeyboardInterrupt:
    print("Interrupting by keyboard.")

    for p_id in processes.keys():
        processes[p_id].kill()
        if processes[p_id].poll() is None:
            print(f"Процесс {p_id} всё ещё выполняется, его PID: {processes[p_id].pid}!")
        else:
            print(f"Процесс {p_id} был завершён!")

    subprocess.run(["docker-compose", "down"])

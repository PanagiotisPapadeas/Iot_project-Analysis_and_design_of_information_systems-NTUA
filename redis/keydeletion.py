import redis

# r = redis.Redis(
#   host='redis-10829.c55.eu-central-1-1.ec2.cloud.redislabs.com',
#   port=10829,
#   password='ZZRzLsLoJODxbmXZREbi1bFiGd3AfsLK')

r = redis.Redis(host='localhost', port=6379, db=0)

r.execute_command("TS.DEL TH1 1500000000000 1600000000000")
r.execute_command("TS.DEL TH2 1500000000000 1600000000000")
r.execute_command("TS.DEL HVAC1 1500000000000 1600000000000")
r.execute_command("TS.DEL HVAC2 1500000000000 1600000000000")
r.execute_command("TS.DEL MiAC1 1500000000000 1600000000000")
r.execute_command("TS.DEL MiAC2 1500000000000 1600000000000")
r.execute_command("TS.DEL Etot 1500000000000 1600000000000")
r.execute_command("TS.DEL Mov1 1500000000000 1600000000000")
r.execute_command("TS.DEL W1 1500000000000 1600000000000")
r.execute_command("TS.DEL Wtot 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayRestEtot 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayRestWtot 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayTH1 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayTH2 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayHVAC1 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayHVAC2 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayMiAC1 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayMiAC2 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayDiffEtot 1500000000000 1600000000000")
r.execute_command("TS.DEL AggDayDiffWtot 1500000000000 1600000000000")
r.execute_command("TS.DEL LateRejW1 1500000000000 1600000000000")
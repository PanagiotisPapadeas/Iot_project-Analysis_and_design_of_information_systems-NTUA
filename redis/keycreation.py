import redis

# r = redis.Redis(
#   host='redis-10829.c55.eu-central-1-1.ec2.cloud.redislabs.com',
#   port=10829,
#   password='ZZRzLsLoJODxbmXZREbi1bFiGd3AfsLK')

r = redis.Redis(host='localhost', port=6379, db=0)

r.execute_command("ts.create TH1")
r.execute_command("ts.create TH2")
r.execute_command("ts.create HVAC1")
r.execute_command("ts.create HVAC2")
r.execute_command("ts.create MiAC1")
r.execute_command("ts.create MiAC2")
r.execute_command("ts.create Etot")
r.execute_command("ts.create Mov1")
r.execute_command("ts.create W1")
r.execute_command("ts.create Wtot")
r.execute_command("ts.create AggDayRestEtot")
r.execute_command("ts.create AggDayRestWtot")
r.execute_command("ts.create AggDayTH1")
r.execute_command("ts.create AggDayTH2")
r.execute_command("ts.create AggDayHVAC1")
r.execute_command("ts.create AggDayHVAC2")
r.execute_command("ts.create AggDayMiAC1")
r.execute_command("ts.create AggDayMiAC2")
r.execute_command("ts.create AggDayDiffEtot")
r.execute_command("ts.create AggDayDiffWtot")
r.execute_command("ts.create LateRejW1")
import datetime as dt
import random as random

t1 = t2 = dt.datetime.now()
date = dt.datetime(2020, 1, 1, 0, 0, 0)
Etot = 1500.8
Wtot = 80.8
while True:
    time = dt.datetime.now()
    delta1 = time - t1
    delta2 = time - t2

    #every 15 minutes
    if delta1.seconds >= 1 :
        TH1val = round(random.uniform(12, 35),1)
        TH2val = round(random.uniform(12, 35),1)
        HVAC1val = round(random.uniform(0, 100),1)
        HVAC2val = round(random.uniform(0, 200),1)
        MiAC1val = round(random.uniform(0, 150),1)
        MiAC2val = round(random.uniform(0, 200),1)
        W1val = round(random.uniform(0, 1),1)
        print("5 sec")
        print("TH1", date, TH1val)
        print("TH2", date, TH2val)
        print("HVAC1", date, HVAC1val)
        print("HVAC2", date, HVAC2val)
        print("MiAC1", date, MiAC1val)
        print("MiAC2", date, MiAC2val)
        print("W1", date, W1val)
        secs = random.randrange(900)
        move_possibility = random.randrange(5)
        if (move_possibility == 0) :
            move_date = date + dt.timedelta(seconds=secs)
            print("Mov1", move_date, 1)
        date = date + dt.timedelta(minutes=15)
        t1 = dt.datetime.now()

    #every 1 day    
    if delta2.seconds >= 96 :
        Etotval = round(random.uniform(-1000, 1000),1)
        Wtotval = round(random.uniform(-10, 10),1)
        print("10 sec")
        print("Etot", date, Etot)
        print("Wtot", date, Wtot)
        Etot = round((Etot + (2600*24) + Etotval),1)
        Wtot = round((Wtot + 110 + Wtotval),1) 
        t2 = t1

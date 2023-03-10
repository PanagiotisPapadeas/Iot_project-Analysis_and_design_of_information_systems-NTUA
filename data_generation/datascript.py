import datetime as dt
import random as random

t1 = dt.datetime.now()
date = dt.datetime(2020, 1, 1, 0, 0, 0)
Etot = 1500.8
Wtot = 80.8
count2 = count3 = count4 = 0
while True:
    time = dt.datetime.now()
    delta1 = time - t1

    #every 15 minutes
    if delta1.seconds >= 1 :
        count2 = count2 + 1
        count3 = count3 + 1
        count4 = count4 + 1
        TH1val = round(random.uniform(12, 35),1)
        TH2val = round(random.uniform(12, 35),1)
        HVAC1val = round(random.uniform(0, 100),1)
        HVAC2val = round(random.uniform(0, 200),1)
        MiAC1val = round(random.uniform(0, 150),1)
        MiAC2val = round(random.uniform(0, 200),1)
        W1val = round(random.uniform(0, 1),1)
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
        
        #every 1 day
        if (count2 >= 5):
            Etotval = round(random.uniform(-1000, 1000),1)
            Wtotval = round(random.uniform(-10, 10),1)
            print("40")
            print("Etot", date, Etot)
            print("Wtot", date, Wtot)
            Etot = round((Etot + (2600*24) + Etotval),1)
            Wtot = round((Wtot + 110 + Wtotval),1)
            count2 = 0

        #every 5 hours    
        if (count3 >= 10):
            W2val = round(random.uniform(0, 1),1)
            delayed_date1 = date - dt.timedelta(hours=48)
            print("20")
            print("W1", delayed_date1, W2val)
            count3 = 0

        #every 30 hours    
        if (count4 >= 15):
            W3val = round(random.uniform(0, 1),1)
            delayed_date2 = date - dt.timedelta(hours=240)
            print("30")
            print("W1", delayed_date2, W3val)
            count4 = 0

        date = date + dt.timedelta(minutes=15)
        t1 = dt.datetime.now()





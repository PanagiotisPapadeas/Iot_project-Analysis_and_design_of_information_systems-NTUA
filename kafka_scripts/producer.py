import datetime as dt
import random as random
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

t1 = dt.datetime.now()
date = dt.datetime(2020, 1, 1, 0, 0, 0)
Etot = 0
Wtot = 0
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
        s1 = bytes("TH1 "+ str(date)+ " "+ str(TH1val),'utf-8')
        s2 = bytes("TH2 "+ str(date)+ " "+ str(TH2val),'utf-8')
        s3 = bytes("HVAC1 " + str(date) + " "+ str(HVAC1val), 'utf-8')
        s4 = bytes("HVAC2 " + str(date) + " "+ str(HVAC2val), 'utf-8')
        s5 = bytes("MiAC1 " + str(date) + " "+ str(MiAC1val), 'utf-8')
        s6 = bytes("MiAC2 " + str(date) + " "+ str(MiAC2val), 'utf-8')
        s7 = bytes("W1 " + str(date) + " "+ str(W1val), 'utf-8')
        producer.send('dailyAggr', s1, partition=0)
        producer.send('dailyAggr', s2, partition=0)
        producer.send('dailyAggr', s3, partition=1)
        producer.send('dailyAggr', s4, partition=1)
        producer.send('dailyAggr', s5, partition=1)
        producer.send('dailyAggr', s6, partition=1)
        producer.send('dailyAggr', s7, partition=3)
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
            s8 = bytes("Mov1 " + str(move_date) + " " + str(1), 'utf-8')
            producer.send('dailyAggr', s8, partition=5)
            print("Mov1", move_date, 1)
        
        #every 1 day
        if (count2 >= 96):
            Etotval = round(random.uniform(-1000, 1000),1)
            Wtotval = round(random.uniform(-10, 10),1)
            print("Day")
            Etot = round((Etot + (2600*24) + Etotval),1)
            Wtot = round((Wtot + 110 + Wtotval),1)
            s9 = bytes("Etot " + str(date) + " "+ str(Etot), 'utf-8')
            s10 = bytes("Wtot " + str(date) + " "+ str(Wtot), 'utf-8')
            producer.send('dailyAggr', s9, partition=2)
            producer.send('dailyAggr', s10, partition=4)
            print("Etot", date, Etot)
            print("Wtot", date, Wtot)
            # Etot = round((Etot + (2600*24) + Etotval),1)
            # Wtot = round((Wtot + 110 + Wtotval),1)
            count2 = 0

        #every 5 hours    
        if (count3 >= 20):
            W2val = round(random.uniform(0, 1),1)
            delayed_date1 = date - dt.timedelta(hours=2)
            print("Late_accept")
            s11 = bytes("W1 " + str(delayed_date1) + " "+ str(W2val), 'utf-8')
            producer.send('dailyAggr', s11, partition=3)
            print("W1", delayed_date1, W2val)
            count3 = 0

        #every 30 hours    
        if (count4 >= 120):
            W3val = round(random.uniform(0, 1),1)
            delayed_date2 = date - dt.timedelta(hours=240)
            print("Late_rej")
            s12 = bytes("W1 " + str(delayed_date2) + " "+ str(W3val), 'utf-8')
            producer.send('lateRej', s12)
            print("W1", delayed_date2, W3val)
            count4 = 0

        date = date + dt.timedelta(minutes=15)
        t1 = dt.datetime.now()





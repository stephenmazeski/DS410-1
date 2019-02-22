##Loading data in Python MRJob
##Remove Hack

from mrjob.job import MRJob

class mainfunc(MRJob):
        def mapper(self, key, value):

                lines = value.split(",")
                medallion = lines[0]
                hack_license = lines[1]
                vendor_id = lines[2]
                pickup_datetime = lines[3]
                payment_type = lines[4]
                fare_amount = lines[5]
                surcharge = lines[6]
                mta_tax = lines[7]
                tip_amount = lines[8]
                tolls_amount = lines[9]
                total_amount = lines[10]
#for each category on list
                Try:
#try to make floats from strings. In turn eliminated null values and headers that were unnecessary intrinsically cleaning the data
                        fare_amount = float(fare_amount)
                        surcharge = float(surcharge)
                        mta_tax = float(mta_tax)
                        tip_amount = float(tip_amount)
                        tolls_amount = float(tolls_amount)
                        total_amount = float(total_amount)
                        yield medallion, (hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, surcharge,mta_tax,tip_amount,tolls_amount,total_amount)
                except:
                        pass

        def reducer(self, key, list):
                for (a,b,c,d,e,f,g,h,i,j) in list:
                        A = a
                        B = b
                        C = c
                        D = d
                        E = e
                        F = f
                        G = g
                        H = h
                        I = i
                        J = j
                yield key,(B,C,D,E,F,G,H,I,J)

if __name__ == '__main__':
        mainfunc.run()

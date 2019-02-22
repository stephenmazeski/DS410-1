from mrjob.job import MRJob

class seconfunc(MRJob):
        def mapper(self, key, value):

                lines = value.split(",")
                medallion = lines[0]
                hack_license = lines[1]
                vendor_id = lines[2]
                rate_code = lines[3]
                store_and_fwd_flag = lines[4]
                pickup_datetime = lines[5]
                dropoff_datetime = lines[6]
                passenger_count = lines[7]
                trip_time = lines[8]
                trip_distance = lines[9]
                pickup_long = lines[10]
                pickup_lat = lines[11]
                drop_long = lines[12]
                drop_lat = lines[13]
#for each category on list
                Try:
#try to make floats from strings. In turn eliminated null values and headers that were unnecessary intrinsically cleaning the data
                        passenger_count= float(passenger_count)
                        trip_time = float(trip_time )
                        trip_distance = float(trip_distance )
                        pickup_long = float(pickup_long )
                        pickup_lat = float(pickup_lat )
                        drop_long = float(drop_long )
drop_lat = float(drop_lat  )

                        yield medallion, (hack_license, vendor_id, rate_code , store_and_fwd_flag , pickup_datetime , dropoff_datetime ,passenger_count ,trip_time ,trip_distance ,pickup_long ,pickup_lat , drop_long , drop_lat )
                except:
                        pass

        def reducer(self, key, list):
                for (a,b,c,d,e,f,g,h,i,j,k,l,m) in list:
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
                        K = k
                        L = l
                        M = m
                yield key,(B,C,D,E,F,G,H,I,J,K,L,M)

if __name__ == '__main__':
        seconfunc.run()

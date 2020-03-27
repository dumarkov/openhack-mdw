-- Databricks notebook source
-- DBTITLE 1,DimActors
truncate table unified.DimActors 
;

insert into unified.DimActors 

select     row_number() over (order by null)         as ActorSK
          ,min((s.src_priority, t.ActorID)).ActorID  as ActorID
          
          ,t.ActorName  
          ,t.Gender     as ActorGender

from      (          select src_id, ActorID, ActorName, Gender  from fourthcoffee_rentals_v.actors
          union all  select src_id, ActorID, ActorName, Gender  from vanarsdelltd_onpremrentals_dbo_v.actors
          ) t
          
          join process_mng.src s on s.src_id = t.src_id  
          
group by  t.ActorName
         ,t.Gender

-- COMMAND ----------

select    f, v, count(*)

from     (select     count(case src when 'f' then 1 end) as f
                   , count(case src when 'v' then 1 end) as v

          from      (          select 'f' as src, ActorName, Gender  from fourthcoffee_rentals_v.actors
                    union all  select 'v' as src, ActorName, Gender  from vanarsdelltd_onpremrentals_dbo_v.actors
                    )

          group by  ActorName, Gender
          )
      
group by  f, v

-- COMMAND ----------

-- DBTITLE 1,dict_actor
drop table if exists unified.dict_actor
;

create table if not exists unified.dict_actor
using delta
as
select     ActorID                                                                                     as from_ActorID
          ,first(ActorID) over (partition by t.ActorName, t.Gender order by s.src_priority, t.ActorID) as to_ActorID
          
from      (          select src_id, ActorID, ActorName, Gender  from fourthcoffee_rentals_v.actors
          union all  select src_id, ActorID, ActorName, Gender  from vanarsdelltd_onpremrentals_dbo_v.actors
          ) t

          join process_mng.src s on s.src_id = t.src_id  

-- COMMAND ----------

-- DBTITLE 1,DimCategories
truncate table unified.DimCategories 
;

insert into unified.DimCategories 

select    row_number() over (order by null)                as MovieCategorySK

         ,case category 
              when 'Science Fact' then 'Science Fiction' 
              else category 
          end                                              as MovieCategoryDescription

from      (            select category from fourthcoffee_rentals_v.movies           
            union all  select category from southridge_southridge_v.movies          
            union all  select category from vanarsdelltd_onpremrentals_dbo_v.movies
          )
      
group by  2      

-- COMMAND ----------

-- DBTITLE 1,DimDate
truncate table unified.DimDate 
;

with t as (select posexplode(sequence(date '1970-01-01', current_date)) as (seq, dt))

insert into unified.DimDate 

select  seq              as DateSK 
       ,dt               as DateValue
       ,year(dt)         as DateYear
       ,month(dt)        as DateMonth
       ,day(dt)          as DateDay
       ,dayofweek(dt)    as DateDayOfWeek
       ,dayofyear(dt)    as DateDayOfYear
       ,weekofyear(dt)   as DateWeekOfYear
       
from    t

-- COMMAND ----------

-- DBTITLE 1,DimCustomers
truncate table unified.DimCustomers 
;

insert into unified.DimCustomers 

select      row_number() over (order by null)             as CustomerSK
           ,min((s.src_priority, CustomerID)).CustomerID  as CustomerID
           
           ,t.LastName		
           ,t.FirstName			
           
           ,inline(array(min((t.AddressLine1, t.AddressLine2, t.City, t.State, t.ZipCode))))

           ,t.PhoneNumber
           ,min(CreatedDate)  as RecordStartDate	
           ,null              as RecordEndDate	
           ,true              as ActiveFlag		

from        (           select  src_id, CustomerID, LastName, FirstName, AddressLine1, AddressLine2, City, State, ZipCode, PhoneNumber, CreatedDate
                        from    fourthcoffee_rentals_v.customers

            union all   select  src_id, CustomerID, LastName, FirstName, AddressLine1, AddressLine2, City, State, ZipCode, PhoneNumber, CreatedDate
                        from    vanarsdelltd_onpremrentals_dbo_v.customers

            union all   select  c.src_id, c.CustomerID, c.LastName, c.FirstName, a.AddressLine1, a.AddressLine2, a.City, a.State, a.ZipCode, c.PhoneNumber, c.CreatedDate
                        from               southridge_cloudsales_dbo_v.customers as c
                                left join  southridge_cloudsales_dbo_v.addresses as a
                                on         a.CustomerID = c.CustomerID

            union all   select  c.src_id, c.CustomerID, c.LastName, c.FirstName, a.AddressLine1, a.AddressLine2, a.City, a.State, a.ZipCode, c.PhoneNumber, c.CreatedDate
                        from               southridge_cloudstreaming_dbo_v.customers as c
                                left join  southridge_cloudstreaming_dbo_v.addresses as a
                                on         a.CustomerID = c.CustomerID
            ) t  
            
            join process_mng.src s on s.src_id = t.src_id  

group by    t.LastName		
           ,t.FirstName		
           ,t.PhoneNumber

-- COMMAND ----------

-- DBTITLE 1,dict_customer
drop table if exists unified.dict_customer
;

create table if not exists unified.dict_customer
using delta
as
select     t.CustomerID                                                                                                          as from_CustomerID
          ,first(t.CustomerID) over (partition by t.LastName, t.FirstName, t.PhoneNumber order by s.src_priority, t.CustomerID)  as to_CustomerID

from      (             select src_id, CustomerID, LastName, FirstName, PhoneNumber from fourthcoffee_rentals_v.customers
            union all   select src_id, CustomerID, LastName, FirstName, PhoneNumber from vanarsdelltd_onpremrentals_dbo_v.customers
            union all   select src_id, CustomerID, LastName, FirstName, PhoneNumber from southridge_cloudsales_dbo_v.customers
            union all   select src_id, CustomerID, LastName, FirstName, PhoneNumber from southridge_cloudstreaming_dbo_v.customers 
          ) t

          join process_mng.src s on s.src_id = t.src_id  

-- COMMAND ----------

-- DBTITLE 1,DimLocations
truncate table unified.DimLocations 
;

insert into unified.DimLocations 

select    row_number() over (order by null)                               as LocationSK
         ,concat_ws(' ',State, City, AddressLine1, AddressLine2, ZipCode) as LocationName
         
         ,max(case store_type when 'st' then true else false end)         as Streaming
         ,max(case store_type when 'rn' then true else false end)         as Rentals
         ,max(case store_type when 'sl' then true else false end)         as Sales

from      (          select 'rn' as store_type, State, City, AddressLine1, AddressLine2, ZipCode from fourthcoffee_rentals_v.customers
          union all  select 'sl' as store_type, State, City, AddressLine1, AddressLine2, ZipCode from southridge_cloudsales_dbo_v.addresses
          union all  select 'st' as store_type, State, City, AddressLine1, AddressLine2, ZipCode from southridge_cloudstreaming_dbo_v.addresses
          union all  select 'rn' as store_type, State, City, AddressLine1, AddressLine2, ZipCode from vanarsdelltd_onpremrentals_dbo_v.customers
          )
          
group by  State, City, AddressLine1, AddressLine2, ZipCode

-- COMMAND ----------

-- DBTITLE 1,DimRatings
truncate table unified.DimRatings 
;

insert into unified.DimRatings

select    row_number() over (order by null)  as MovieRatingSK
         ,nvl(pmr.norm_rating, t.rating)     as MovieRatingDescription 

from      (            select rating from fourthcoffee_rentals_v.movies           
            union all  select rating from southridge_southridge_v.movies          
            union all  select rating from vanarsdelltd_onpremrentals_dbo_v.movies
          ) t
          
          left join process_mng.rating pmr on pmr.original_rating = t.rating
      
group by  2      

-- COMMAND ----------

-- DBTITLE 1,DimMovies
truncate table unified.DimMovies 
;

insert into unified.DimMovies 

select      row_number() over (order by null)    as MovieSK		
           ,MovieID
           
           ,t.MovieTitle		
           ,c.MovieCategorySK
           ,r.MovieRatingSK	
           ,t.RunTimeMin       as MovieRunTimeMin
           
from       (select      row_number() over (partition by t.MovieTitle order by s.src_priority, t.MovieID) as rn			
                       ,t.*

            from        (           select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from fourthcoffee_rentals_v.movies
                        union all   select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from southridge_southridge_v.movies
                        union all   select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from vanarsdelltd_onpremrentals_dbo_v.movies
                        ) t
            
                        join process_mng.src s on s.src_id = t.src_id  
            ) t
            
            left join process_mng.rating pmr on pmr.original_rating = t.rating
            
            join unified.DimRatings r on r.MovieRatingDescription = nvl(pmr.norm_rating, t.rating)
                
            join unified.DimCategories c on c.MovieCategoryDescription = t.Category
            
where       rn = 1            

-- COMMAND ----------

select      *

from       (select      count(*) over (partition by MovieTitle)                                                                         as records_cnt
                       ,cardinality(collect_set((Category, nvl(pmr.norm_rating, t.rating), RunTimeMin)) over (partition by MovieTitle)) as variations_cnt 
                       ,row_number () over (partition by MovieTitle order by src_id, MovieID)                                           as record_seq
                       ,dense_rank () over (partition by MovieTitle order by (Category, Rating, RunTimeMin))                            as variation_seq
                       ,t.*

            from        (           select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from fourthcoffee_rentals_v.movies
                        union all   select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from southridge_southridge_v.movies
                        union all   select src_id, MovieID, MovieTitle, Category, Rating, RunTimeMin from vanarsdelltd_onpremrentals_dbo_v.movies
                        ) t
                        
                        left join process_mng.rating pmr on pmr.original_rating = t.rating
            ) t
            
where       variations_cnt > 1

order by    MovieTitle
           ,record_seq    
           
                      

-- COMMAND ----------

-- DBTITLE 1,dict_movie
drop table if exists unified.dict_movie
;

create table if not exists unified.dict_movie
using delta
as
select      MovieID                                                                             as from_MovieID		
           ,first(MovieID) over (partition by t.MovieTitle order by s.src_priority, t.MovieID)  as to_MovieID

from        (           select src_id, MovieID, MovieTitle from fourthcoffee_rentals_v.movies
            union all   select src_id, MovieID, MovieTitle from southridge_southridge_v.movies
            union all   select src_id, MovieID, MovieTitle from vanarsdelltd_onpremrentals_dbo_v.movies
            ) t

            join process_mng.src s on s.src_id = t.src_id

-- COMMAND ----------

-- DBTITLE 1,DimMovieActors
truncate table unified.DimMovieActors 
;

insert into unified.DimMovieActors 

select    d_m.to_MovieID  as MovieID
         ,d_a.to_ActorID  as ActorID

from      (           select MovieID, ActorID from fourthcoffee_rentals_v.movieactors
          union all   select MovieID, ActorID from vanarsdelltd_onpremrentals_dbo_v.movieactors
          ) t
          join unified.dict_movie as d_m on d_m.from_MovieID = t.MovieID
          join unified.dict_actor as d_a on d_a.from_ActorID = t.ActorID
          
group by  1, 2

-- COMMAND ----------

-- DBTITLE 1,DimTime
truncate table unified.DimTime 
;

with t as (select posexplode(sequence(timestamp '1970-01-01 00:00:00', timestamp '1970-01-01 23:59:59', interval 1 second)) as (seq, ts))

insert into unified.DimTime

select  seq                as TimeSK 
       ,substr(ts, 12, 8)  as TImeValue
       ,hour(ts)           as hour
       ,minute(ts)         as TimeMinute
       ,second(ts)         as TimeSecond
       ,floor(seq / 60)    as TimeMinuteOfDay
       ,seq                as TimeSecondOfDay 
       
from    t

-- COMMAND ----------

-- DBTITLE 1,FactRentals
truncate table unified.FactRentals
;

insert into unified.FactRentals

select  row_number() over (order by null)     as RentalSK
       ,t.TransactionID                        
       ,dc.CustomerSK  
       ,dl.LocationSK  
       ,dm.MovieSK                            
       ,dd_rntl.DateSK                        as RentalDateSK           
       ,dd_rtrn.DateSK                        as ReturnDateSK
       ,datediff(t.ReturnDate, t.RentalDate)  as RentalDuration
       ,RentalCost                            
       ,LateFee                               
       ,RentalCost + LateFee                  as TotalCost
       ,RewindFlag                            
       
from    (           select TransactionID, CustomerID, MovieID, RentalDate, ReturnDate, RentalCost, LateFee, RewindFlag  from fourthcoffee_rentals_v.transactions
        union all   select TransactionID, CustomerID, MovieID, RentalDate, ReturnDate, RentalCost, LateFee, RewindFlag  from vanarsdelltd_onpremrentals_dbo_v.transactions
        ) t
        
        left join unified.dict_customer as d_c     on d_c.from_CustomerID = t.CustomerID
        left join unified.DimCustomers  as dc      on dc.CustomerID       = d_c.to_CustomerID
        left join unified.DimLocations  as dl      on dl.LocationName     = concat_ws(' ', dc.State, dc.City, dc.AddressLine1, dc.AddressLine2, dc.ZipCode)
         
        left join unified.dict_movie    as d_m     on d_m.from_MovieID    = t.MovieID
        left join unified.DimMovies     as dm      on dm.MovieID          = d_m.to_MovieID
        
        left join unified.DimDate       as dd_rntl on dd_rntl.DateValue   = t.RentalDate
        left join unified.DimDate       as dd_rtrn on dd_rtrn.DateValue   = t.ReturnDate
        

-- COMMAND ----------

-- DBTITLE 1,FactSales
truncate table unified.FactSales
;

insert into unified.FactSales

select  row_number() over (order by null)     as SalesSK
       ,o.OrderID
       ,od.LineNumber
       ,dd_ordr.DateSK                as OrderDateSK
       ,dd_shp.DateSK                 as ShipDateSK
       ,dc.CustomerSK
       ,dl.LocationSK
       ,dm.MovieSK
       ,datediff(ShipDate, OrderDate) as DaysToShip
       ,od.Quantity
       ,od.UnitCost
       ,od.Quantity * od.UnitCost     as ExtendedCost
 
from              southridge_cloudsales_dbo_v.orders       as o
        join      southridge_cloudsales_dbo_v.orderdetails as od  on od.OrderID = o.OrderID

        left join unified.DimDate       as dd_ordr on dd_ordr.DateValue   = o.OrderDate
        left join unified.DimDate       as dd_shp  on dd_shp.DateValue    = o.ShipDate
        
        left join unified.dict_customer as d_c     on d_c.from_CustomerID = o.CustomerID
        left join unified.DimCustomers  as dc      on dc.CustomerID       = d_c.to_CustomerID
        left join unified.DimLocations  as dl      on dl.LocationName     = concat_ws(' ', dc.State, dc.City, dc.AddressLine1, dc.AddressLine2, dc.ZipCode)
        
        left join unified.dict_movie    as d_m     on d_m.from_MovieID    = od.MovieID
        left join unified.DimMovies     as dm      on dm.MovieID          = d_m.to_MovieID        

-- COMMAND ----------

-- DBTITLE 1,FactStreaming
truncate table unified.FactStreaming
;

insert into unified.FactStreaming

select  row_number() over (order by null)     as StreamingSK
 
        ,t.TransactionID
        ,dc.CustomerSK
        ,dm.MovieSK
        
        ,dd_ss.DateSK       as StreamStartDateSK
        ,dt_ss.TimeSK       as StreamStartTimeSK
        ,dd_se.DateSK       as StreamEndDateSK
        ,dt_se.TimeSK       as StreamEndTimeSK
        
        ,to_unix_timestamp(StreamEnd) - to_unix_timestamp(StreamStart)                             as StreamDurationSec
        ,cast(to_unix_timestamp(StreamEnd) - to_unix_timestamp(StreamStart) / 60 as decimal(14,4)) as StreamDurationMin

from              southridge_cloudstreaming_dbo_v.transactions as t

        left join unified.DimDate       as dd_ss   on dd_ss.DateValue    = to_date(t.StreamStart)
        left join unified.DimDate       as dd_se   on dd_se.DateValue    = to_date(t.StreamEnd)
        
        left join unified.DimTime       as dt_ss   on dt_ss.TimeValue    = substr(t.StreamStart, 12, 8)
        left join unified.DimTime       as dt_se   on dt_se.TimeValue    = substr(t.StreamEnd,   12, 8)      
        
        left join unified.dict_customer as d_c     on d_c.from_CustomerID = t.CustomerID
        left join unified.DimCustomers  as dc      on dc.CustomerID       = d_c.to_CustomerID
        
        left join unified.dict_movie    as d_m     on d_m.from_MovieID    = t.MovieID
        left join unified.DimMovies     as dm      on dm.MovieID          = d_m.to_MovieID    

-- COMMAND ----------

-- DBTITLE 1,Generate unified display
-- MAGIC %python
-- MAGIC 
-- MAGIC rows_per_table = 10
-- MAGIC 
-- MAGIC cell_sep = '\n\n' '-- COMMAND ----------' '\n'
-- MAGIC 
-- MAGIC for t in sql(f"show tables in unified").collect():
-- MAGIC 
-- MAGIC   print(f'-- DBTITLE 1,{t.tableName}')
-- MAGIC   print(f'select * from unified.{t.tableName} limit {rows_per_table}{cell_sep}')
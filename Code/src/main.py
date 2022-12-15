from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import pyspark.sql.window import Window

import os
import sys

if os.path.exists('src.zip'):
    os.path.insert(0,'src.zip')
else:
    os.path.insert(0,'./utilities')


class CarCrashAnalysis:
    
    def __init__( self, config_file_path):
       input_file_path=util.read_yaml('config_file_path').get('INPUT_FILE_NAME')
       self.charges_df=utils.load_data(spark,input_file_path.get('Charges_use'))
       self.damages_df=utils.load_data(spark,input_file_path.get('Damages_use'))
       self.endorse_df=utils.load_data(spark,input_file_path.get('Endorse_use'))
       self.primary_person_df=utils.load_data(spark,input_file_path.get('Primary_Person_use'))
       self.restrict_df=utils.load_data(spark,input_file_path.get('Restrict_use'))
       self.units_df=utils.load_data(spark,input_file_path.get('Units_use'))
    
    def count_killed_male_persons(self,output_file_path,output_file_format):
      '''Finds the crashes (accidents) in which number of persons killed are male
        param output_file_path: output file path
        param output_file_format: output file format
        return: Dataframe Count'''
        
        count_killed_male_persons_df=self.primary_person_df.filter(F.col('PRSN_GNDR_ID')=='MALE')
        count_killed_male_persons_df=count_killed_male_persons_df.dropDuplicates()
        utils.write_output(count_killed_male_persons_df,output_file_path,output_file_format)
        return(count_killed_male_persons_df.count()) 
    
    #Count of two-wheelers are booked for crashes
    def  count_two_wheelers_crash(self,output_file_path,output_file_format):

      '''Finds the crashes where the vehicle type was 2 wheeler.
        param output_file_path: output file path
        param output_file_format: output file format
        return: Dataframe Count'''
        
        count_two_wheelers_crash_df=self.units_df.filter(F.col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))
        count_two_wheelers_crash_df=count_two_wheelers_crash_df.dropDuplicates()
        utils.write_output(count_two_wheelers_crash_df,output_file_path,output_file_format)
        return(count_two_wheelers_crash_df.count())

    def  state_with_highest_female_casualties(self,output_file_path,output_file_format):
     '''Finds state name with highest female accidents
        param output_file_path: output file path
        param output_file_format: output file format
        return: State Name with Highest Female Accidents'''

        state_with_highest_female_casualties_df=self.primary_person_df.filter(F.col('PRSN_GNDR_ID')=='FEMALE')
        state_with_highest_female_casualties_df=state_with_highest_female_casualties_df.groupBy('DRVR_LIC_STATE_ID')\
                                                                                    .agg(F.count('*').alias('count'))
        state_with_highest_female_casualties_df=state_with_highest_female_casualties_df.orderBy(F.col('count').desc())   
       
        utils.write_output(state_with_highest_female_casualties_df,output_file_path,output_file_format)
        return (state_with_highest_female_casualties_df.first()['DRVR_LIC_STATE_ID'])
    
    
    def vehmake_ids_contributing_to_injuries(self,output_file_path,output_file_format):
     '''Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        param output_file_path: output file path
        param output_file_format: output file format
        return: List of Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death'''
        
        vehmake_ids_contributing_to_injuries_df=self.units_df.filter(F.col('VEH_MAKE_ID')!='NA')
        vehmake_ids_contributing_to_injuries_df=vehmake_ids_contributing_to_injuries_df.withColumn('Total_Casualties',F.col('TOT_INJRY_CNT')+F.col('DEATH_CNT'))
        vehmake_ids_contributing_to_injuries_df=vehmake_ids_contributing_to_injuries_df.groupBy('VEH_MAKE_ID').agg(F.sum('Total_Casualties').alias('Total_Casualties'))
        w=Window.orderBy(F.col('Total_Casualties').desc())
        vehmake_ids_contributing_to_injuries_df=vehmake_ids_contributing_to_injuries_df.withColumn('rank',F.dense_rank().over(w))
        vehmake_ids_contributing_to_injuries_df=vehmake_ids_contributing_to_injuries_df.filter(F.col('rank').between(5,15))
        
        vehmake_ids_contributing_to_injuries_df=vehmake_ids_contributing_to_injuries_df.dropDuplicates()
        utils.write_output(vehmake_ids_contributing_to_injuries_df,output_file_path,output_file_format)
        return ([a[0] for a in vehmake_ids_contributing_to_injuries_df.select('VEH_MAKE_ID').collect()])
    
    def top_ethnic_user_group(self,output_file_path,output_file_format):
      '''Finds and show top ethnic user group of each unique body style that was involved in crashes
        param output_file_path: output file path
        param output_file_format: output file format
        return: Dataframe with Top Ethnic User Group of Each Unique Body Style '''

        filter_primary_person_df=self.primary_person_df.filter(~F.col('PRSN_ETHNICITY_ID').isin(['NA', 'UNKNOWN']))
        filter_units_df=self.units_df.filter(~F.col('VEH_BODY_STYL_ID').isin(['NA', 'UNKNOWN', 'NOT REPORTED','OTHER  (EXPLAIN IN NARRATIVE)']))
        join_df=filter_primary_person_df.join(filter_units_df,on='CRASH_ID',how='inner')
        join_df1=join_df.groupBy(['PRSN_ETHNICITY_ID','VEH_BODY_STYL_ID']).agg(F.count('*').alias('count'))
        
        w=Window.partitionBy('VEH_BODY_STYL_ID').orderBy(F.col('count').desc())
        top_ethnic_user_group_df=join_df1.withColumn('rank',F.rank().over(w)).where(F.col('rank')==1)
        
        top_ethnic_user_group_df=top_ethnic_user_group_df.dropDuplicates()
        utils.write_output(top_ethnic_user_group_df,output_file_path,output_file_format)
        return (top_ethnic_user_group_df.show(truncate=False))

    def zip_code_with_alocohol_as_factor(self,output_file_path,output_file_format):
     '''Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        param output_file_path: output file path
        param output_file_format: output file format
        return: List of Zip Codes'''
        
        filter_units_df=self.units_df.filter((F.col('CONTRIB_FACTR_1_ID').contains('ALCOHOL')) | (F.col('CONTRIB_FACTR_2_ID').contains('ALCOHOL')))
        filter_primary_person_df=self.primary_person_df.dropna(subset='DRVR_ZIP')
        join_df=filter_primary_person_df.join(filter_units_df,on='CRASH_ID',how='inner')
        zip_code_with_alocohol_as_factor_df=join_df.groupBy('DRVR_ZIP').agg(F.count('*').alias('count'))\
                                                    .orderBy(F.col('count').desc()).limit(5)
        
        zip_code_with_alocohol_as_factor_df=zip_code_with_alocohol_as_factor_df.dropDuplicates()
        utils.write_output(zip_code_with_alocohol_as_factor_df,output_file_path,output_file_format)
        return ([a[0] for a in zip_code_with_alocohol_as_factor_df.select('DRVR_ZIP').collect()])


    def vehicle_with_no_damaged_property(self,output_file_path,output_file_format):
    ''' Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
        param output_file_path: output file path
        param output_file_format: output file format
        return: Count of Distinct Crash ids'''

        filter_units_df=self.units_df.filter(((F.col('VEH_DMAG_SCL_1_ID')>'DAMAGED 4') & (~F.col('VEH_DMAG_SCL_1_ID').isin(['NA', 'NO DAMAGE', 'INVALID VALUE']))) |
                                            ((F.col('VEH_DMAG_SCL_2_ID')>'DAMAGED 4') & (~F.col('VEH_DMAG_SCL_2_ID').isin(['NA', 'NO DAMAGE', 'INVALID VALUE']))))\
                                            .filter(F.col('FIN_RESP_TYPE_ID') == 'PROOF OF LIABILITY INSURANCE')
        
        filter_damages_df=self.damages_df.filter(F.col('DAMAGED_PROPERTY')=='NONE')
        join_df=filter_units_df.join(filter_damages_df,on='CRASH_ID',how='inner')
        vehicle_with_no_damaged_property_df=join_df.dropDuplicates()

        utils.write_output(vehicle_with_no_damaged_property_df,output_file_path,output_file_format)
        count=vehicle_with_no_damaged_property_df.select(F.countDistinct('CRASH_ID')).collect()[0][0]
        return(count)
   
    def top_vehicle_brands(self,output_file_path,output_file_format):
     '''Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        param output_file_path: output file path
        param output_file_format: output file format
        return List of Vehicle Brands'''

        top_25_state_df=self.units_df.groupBy('VEH_LIC_STATE_ID').agg(F.count('*').alias('count'))\
                                                                    .orderBy(F.col('count').desc()).limit(25)
        state_list=[a[0] for a in top_25_state_df.collect()]

        top_10_vahicle_color_df=self.units_df.groupBy('VEH_COLOR_ID').agg(F.count('*').alias('count'))\
                                                                    .orderBy(F.col('count').desc()).limit(10)    
        vehicle_list=[a[0] for a in top_10_vahicle_color_df.collect()] 
        
        filter_unit_df=self.units_df.filter((F.col('VEH_LIC_STATE_ID').isin(state_list)) & (F.col('VEH_COLOR_ID').isin(vehicle_list)))
        filter_charges_df=self.charges_df.filter(F.col('CHARGE').contains('SPEED'))   
        filter_person_df=self.primary_person_df (F.col('DRVR_LIC_TYPE_ID').isin(['DRIVER LICENSE','COMMERCIAL DRIVER LIC.']))

        filter_charges_df=self.charges_df.filter(F.col('CHARGE').contains('SPEED'))   
        join_df=filter_unit_df.join(filter_charges_df,on='CRASH_ID',how='INNER').join(filter_person_df,on='CRASH_ID',how='inner')

        top_vehicle_brands_df=join_df.groupBy('VEH_MAKE_ID').agg(F.count('*').alias('count')).orderBy(F.col('count').desc()).limit(5)
        utils.write_output(top_vehicle_brands_df,output_file_path,output_file_format)
        return ([a[0] for a in top_vehicle_brands_df.select('VEH_MAKE_ID').collect()])

if __name__=="__main__":
    
    spark=SparkSession.builder.appName('CarCrashAnalysis').getOrCreate()

    config_file_path='config.yaml'
    car_crash_analysis=CarCrashAnalysis(config_file_path)

    output_file_path=utils.read_yaml('config_file_path').get('OUTPUT_FILE_PATH')
    output_file_format=utils.read_yaml('config_file_path').get('OUTPUT_FILE_FORMAT')

    # Number of crashes(Accidents) in which number of persons killed are male:
    print("Number of Persons killed are male", car_crash_analysis.count_killed_male_persons(output_file_path.get('Path1'),output_file_format.get('Format')))

    # Number two-wheelers are booked for crashes:
    print('Number of two-wheelers booked for crashes', car_crash_analysis.count_two_wheelers_crash(output_file_path.get('Path2'),output_file_format.get('Format')))

    #State has highest number of accidents in which females are involved
    print('Number of two-wheelers booked for crashes', car_crash_analysis.state_with_highest_female_casualties(output_file_path.get('Path3'),output_file_format.get('Format')))   

    # Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print('Top 5th to 15th VEH_MAKE_IDs contribute to death', car_crash_analysis.vehmake_ids_contributing_to_injuries(output_file_path.get('Path4'),output_file_format.get('Format')))   

    #For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    print('Top Ethnic User group of Each Unique Body Style', car_crash_analysis.top_ethnic_user_group(output_file_path.get('Path5'),output_file_format.get('Format')))

    #Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    print('Top 5 Zip Code with Alocohol as factor', car_crash_analysis.zip_code_with_alocohol_as_factor(output_file_path.get('Path6'),output_file_format.get('Format')))

    #Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print('Count of Distinct Crash IDs Where No Damaged Property was Observed:' car_crash_analysis.vehicle_with_no_damaged_property(output_file_path.get('Path7'),output_file_format.get('Format')))

    # Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
    # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    print('Top 5 Vehicle Makes/Brands', car_crash_analysis.top_vehicle_brands(output_file_path.get('Path8'),output_file_format.get('Format')))

    spark.stop()


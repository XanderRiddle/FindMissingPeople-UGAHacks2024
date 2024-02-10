IMPORT $;
HMK := $.File_AllData;

OUTPUT(HMK.unemp_ratesDS,NAMED('US_UnempByMonth'));
OUTPUT(CHOOSEN(HMK.unemp_byCountyDS,3000),NAMED('Unemployment'));
OUTPUT(HMK.EducationDS,NAMED('Education'));
OUTPUT(HMK.pov_estimatesDS,NAMED('Poverty'));
OUTPUT(HMK.pop_estimatesDS,NAMED('Population'));
OUTPUT(HMK.PoliceDS,NAMED('Police'));
OUTPUT(HMK.FireDS,NAMED('Fire'));
OUTPUT(HMK.HospitalDS,NAMED('Hospitals'));
OUTPUT(HMK.ChurchDS,NAMED('Churches'));
OUTPUT(HMK.FoodBankDS,NAMED('FoodBanks'));
OUTPUT(CHOOSEN(HMK.mc_byStateDS,3500),NAMED('NCMEC'));
OUTPUT(COUNT(HMK.mc_byStateDS),NAMED('NCMEC_Cnt'));
OUTPUT(HMK.City_DS,NAMED('Cities'));
OUTPUT(COUNT(HMK.City_DS),NAMED('Cities_Cnt'));
OUTPUT(HMK.unemp_byCountyDS(Attribute = 'Unemployment_rate_2022'),NAMED('Unemployment_Rates'));
//OUTPUT(JOIN(HMK.mc_byStateDS, HMK.City_DS, LEFT.missingcity = RIGHT.city AND LEFT.missingstate = RIGHT.state_id),NAMED('Joined_Unemployment_County_Fips'));

County_Fips_REC := RECORD 
  STRING county_fips;
END;


County_Fips_REC County_Fips_Transform(County_Fips_Joined CFJ) := TRANSFORM
    SELF.county_fips := CFJ.county_fips;
END;

County_Fips_Joined := 
JOIN( 
    HMK.mc_byStateDS,
    HMK.City_DS,
    LEFT.missingcity = RIGHT.city AND LEFT.missingstate = RIGHT.state_id
    County_Fips_REC(LEFT,RIGHT),LEFT OUTER)
;

OUTPUT(County_Fips_REC,NAMED('County_Fips'))
/*
County_Fips_Temp := TRANSFORM
  JOIN(
  HMK.mc_byStateDS,
  HMK.City_DS,
  LEFT.missingcity = RIGHT.city AND LEFT.missingstate = RIGHT.state_id)
;

Clean_County_Fips := PROJECT(County_Fips_Temp,TRANSFORM(County_Fips_REC,
  SELF.county_fips := County_Fips_Temp.county_fips
 ,));

OUTPUT(Clean_County_Fips,NAMED('County_fips'));
*/
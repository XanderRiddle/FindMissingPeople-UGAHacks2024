IMPORT $;
IMPORT std;
HMK := $.File_AllData;

//OUTPUT(HMK.unemp_ratesDS,NAMED('US_UnempByMonth'));
//OUTPUT(CHOOSEN(HMK.unemp_byCountyDS,3000),NAMED('Unemployment'));
//OUTPUT(HMK.EducationDS,NAMED('Education'));
//OUTPUT(HMK.pov_estimatesDS,NAMED('Poverty'));
//OUTPUT(HMK.pop_estimatesDS,NAMED('Population'));
//OUTPUT(HMK.PoliceDS,NAMED('Police'));
//OUTPUT(HMK.FireDS,NAMED('Fire'));
//OUTPUT(HMK.HospitalDS,NAMED('Hospitals'));
//OUTPUT(HMK.ChurchDS,NAMED('Churches'));
//OUTPUT(HMK.FoodBankDS,NAMED('FoodBanks'));
//OUTPUT(CHOOSEN(HMK.mc_byStateDS,3500),NAMED('NCMEC'));
//OUTPUT(COUNT(HMK.mc_byStateDS),NAMED('NCMEC_Cnt'));
//OUTPUT(HMK.City_DS,NAMED('Cities'));
//OUTPUT(COUNT(HMK.City_DS),NAMED('Cities_Cnt'));
//OUTPUT(HMK.unemp_byCountyDS(Attribute = 'Unemployment_rate_2022'),NAMED('Unemployment_Rates'));
//OUTPUT(JOIN(HMK.mc_byStateDS, HMK.City_DS, LEFT.missingcity = RIGHT.city AND LEFT.missingstate = RIGHT.state_id),NAMED('Joined_Unemployment_County_Fips'));

County_Fips_Of_Missing_Children_Record := RECORD 
  STRING county_fips;
END;


County_Fips_Of_Missing_Children_Record County_Fips_Transform(HMK.mc_byStateDS Le,HMK.City_DS Ri) := TRANSFORM
    SELF.county_fips := (STRING)Ri.county_fips;
END;

County_Fips_Of_Missing_Children := JOIN(HMK.mc_byStateDS,HMK.City_DS,
                                        LEFT.missingcity = std.str.toUpperCase(RIGHT.city) AND 
                                        LEFT.missingstate = RIGHT.state_id,
                                        County_Fips_Transform(LEFT,RIGHT));

OUTPUT(County_Fips_Of_Missing_Children,NAMED('County_Fips'));

CT_FIPS := TABLE(County_Fips_Of_Missing_Children,{County_Fips_Of_Missing_Children,number_of_missing_children := COUNT(GROUP)},county_fips);
OUTPUT(SORT(CT_FIPS,-number_of_missing_children),NAMED('MissByFIPS'));

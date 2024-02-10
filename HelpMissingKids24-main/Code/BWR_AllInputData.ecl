IMPORT $;
IMPORT std;
HMK := $.File_AllData;

//OUTPUT(HMK.unemp_ratesDS,NAMED('US_UnempByMonth'));
//OUTPUT(CHOOSEN(HMK.unemp_byCountyDS,3000),NAMED('Unemployment'));
OUTPUT(HMK.EducationDS,NAMED('Education'));
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
//OUTPUT(HMK.unemp_byCountyDS(Attribute = 'Unemployment_rate_2022'),NAMED('Ri'));
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
Children_Per_Fip := OUTPUT(SORT(CT_FIPS,-number_of_missing_children),NAMED('MissByFIPS'));

CT_FIPS_Unemployment_Record := RECORD
  STRING county_fip;
  INTEGER number_of_missing_children;
  DECIMAL unemployment_rates;
 END;
 
 CT_FIPS_Unemployment_Record CT_FIPS_Unemployment_Transform(CT_FIPS Le, HMK.unemp_byCountyDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.number_of_missing_children := (INTEGER)Le.number_of_missing_children;
  SELF.unemployment_rates := (DECIMAL)Ri.value;
 END;
 
 
 CT_FIPS_Unemployment := JOIN(CT_FIPS, HMK.unemp_byCountyDS(Attribute = 'Unemployment_rate_2022'),
                              LEFT.county_fips = (STRING)RIGHT.fips_code,
                              CT_FIPS_Unemployment_Transform(LEFT,RIGHT));
                              
OUTPUT(SORT(CT_FIPS_Unemployment,-number_of_missing_children),NAMED('CT_FIPS_Unemployment'));
OUTPUT(CORRELATION(SORT(CT_FIPS_Unemployment,-number_of_missing_children), number_of_missing_children, unemployment_rates),NAMED('unemployment_correlation'));

CT_FIPS_Education_Record := RECORD
  STRING county_fip;
  INTEGER number_of_missing_children;
  INTEGER no_education;
END;

CT_FIPS_Education_Record CT_FIPS_Education_Transform(CT_FIPS Le, HMK.EducationDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.number_of_missing_children := (INTEGER)Le.number_of_missing_children;
  SELF.no_education := (INTEGER)Ri.value;
END;

CT_FIPS_Education := JOIN(CT_FIPS, HMK.EducationDS(attribute = 'Less than a high school diploma, 2017-21'),
                          LEFT.county_fips = (STRING)RIGHT.fips_code,
                          CT_FIPS_Education_Transform(LEFT,RIGHT));

OUTPUT(SORT(CT_FIPS_Education,-number_of_missing_children),NAMED('CT_FIPS_Education'));
OUTPUT(CORRELATION(SORT(CT_FIPS_Education,-number_of_missing_children),number_of_missing_children,no_education),NAMED('education_correlation'));

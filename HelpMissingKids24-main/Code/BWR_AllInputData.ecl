
IMPORT $;
IMPORT std;
HMK := $.File_AllData;

//OUTPUT(HMK.unemp_ratesDS,NAMED('US_UnempByMonth'));
//OUTPUT(CHOOSEN(HMK.unemp_byCountyDS,3000),NAMED('Unemployment'));

//OUTPUT(HMK.EducationDS,NAMED('Education'));
//OUTPUT(HMK.pov_estimatesDS,NAMED('Poverty'));
//OUTPUT(HMK.EducationDS,NAMED('Education'));
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

//OUTPUT(County_Fips_Of_Missing_Children,NAMED('County_Fips'));

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
                              
//OUTPUT(SORT(CT_FIPS_Unemployment,-number_of_missing_children),NAMED('CT_FIPS_Unemployment'));
Unemployment_Correlation := CORRELATION(SORT(CT_FIPS_Unemployment,-number_of_missing_children), number_of_missing_children, unemployment_rates);
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

//OUTPUT(SORT(CT_FIPS_Education,-number_of_missing_children),NAMED('CT_FIPS_Education'));
Education_Correlation := CORRELATION(SORT(CT_FIPS_Education,-number_of_missing_children),number_of_missing_children,no_education);

CT_FIPS_Poverty_Record := RECORD
  STRING county_fip;
  INTEGER number_of_missing_children;
  DECIMAL poverty_nums;
 END;
 
 CT_FIPS_Poverty_Record CT_FIPS_Poverty_Transform(CT_FIPS Le, HMK.pov_estimatesDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.number_of_missing_children := (INTEGER)Le.number_of_missing_children;
  SELF.poverty_nums := (DECIMAL)Ri.value;
 END;
 
 
 CT_FIPS_Poverty := JOIN(CT_FIPS, HMK.pov_estimatesDS(Attribute = 'POVALL_2021'),
                              LEFT.county_fips = (STRING)RIGHT.fips_code,
                              CT_FIPS_Poverty_Transform(LEFT,RIGHT));
                              
//OUTPUT(SORT(CT_FIPS_Poverty,-number_of_missing_children),NAMED('CT_FIPS_Poverty'));
Poverty_Correlation := CORRELATION(SORT(CT_FIPS_Poverty,-number_of_missing_children), number_of_missing_children, poverty_nums);

CT_FIPS_Population_Record := RECORD
  STRING county_fip;
  INTEGER number_of_missing_children;
  INTEGER population;
 END;
 
 CT_FIPS_Population_Record CT_FIPS_Population_Transform(CT_FIPS Le, HMK.pop_estimatesDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.number_of_missing_children := (INTEGER)Le.number_of_missing_children;
  SELF.population := (INTEGER)Ri.value;
END;

CT_FIPS_Population := JOIN(CT_FIPS, HMK.pop_estimatesDS(attribute = 'POP_ESTIMATE_2022'),
  LEFT.county_fips = (STRING)RIGHT.fips_code,
  CT_FIPS_Population_Transform(LEFT,RIGHT));
  

//OUTPUT(SORT(CT_FIPS_Population,-number_of_missing_children),NAMED('CT_FIPS_Population'));
Population_Correlation := CORRELATION(SORT(CT_FIPS_Population,-number_of_missing_children),number_of_missing_children,population);

Unemployment_Normalization_Record := RECORD
  String county_fip;
  DECIMAL normalized_unemployment;
END;

Unemployment_Normalization_Record Unemployement_Normaliztion_Transform(HMK.City_DS Le, HMK.unemp_byCountyDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.normalized_unemployment := ((Ri.value - 0.6)/(13-0.6))*100;
END;

Unemployment_Normalization := JOIN(HMK.City_DS, HMK.unemp_byCountyDS(Attribute = 'Unemployment_rate_2022'),
  LEFT.county_fips = (STRING)RIGHT.fips_code,
  Unemployement_Normaliztion_Transform(LEFT,RIGHT));
  
Unemployment_Normalization_Result := DEDUP(SORT(Unemployment_Normalization, -normalized_unemployment));

Education_Normalization_Record := RECORD
  String county_fip;
  DECIMAL normalized_education;
END;

Education_Normalization_Record Education_Normalization_Transform(HMK.City_DS Le, HMK.EducationDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.normalized_education := ((Ri.value - 2)/(545847-2))*100;
END;

Education_Normalization := JOIN(HMK.City_DS, HMK.EducationDS(attribute = 'Less than a high school diploma, 2017-21'),
  LEFT.county_fips = (STRING)RIGHT.fips_code,
  Education_Normalization_Transform(LEFT,RIGHT));
  
Education_Normalization_Result := DEDUP(SORT(Education_Normalization, -normalized_education));

Poverty_Normalization_Record := RECORD
  STRING county_fip;
  DECIMAL normalized_poverty;
END;

Poverty_Normalization_Record Poverty_Normalization_Transform(HMK.City_DS Le, HMK.pov_estimatesDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.normalized_poverty := (DECIMAL)((Ri.value - 3)/(767505-3))*100;
END;

Poverty_Normalization := JOIN(HMK.City_DS, HMK.pov_estimatesDS(Attribute = 'POVALL_2021'),
  LEFT.county_fips = (STRING)RIGHT.fips_code,
  Poverty_Normalization_Transform(LEFT,RIGHT));
  
Poverty_Normalization_Result := DEDUP(SORT(Poverty_Normalization, -normalized_poverty));

Population_Normalization_Record := RECORD
  STRING county_fip;
  DECIMAL normalized_population;
END;

Population_Normalization_Record Population_Normalization_Transform(HMK.City_DS Le, HMK.pop_estimatesDS Ri) := TRANSFORM
  SELF.county_fip := Le.county_fips;
  SELF.normalized_population := ((Ri.value - 51)/(5109292-51))*100;
END;

Population_Normalization := JOIN(HMK.City_DS, HMK.pop_estimatesDS(attribute = 'POP_ESTIMATE_2022'),
  LEFT.county_fips = (STRING)RIGHT.fips_code,
  Population_Normalization_Transform(LEFT,RIGHT));
  
Population_Normalization_Result := DEDUP(SORT(Population_Normalization, -normalized_population));

DECIMAL SUM_OF_CORRELATIONS := Unemployment_Correlation + Education_Correlation + Poverty_Correlation + Population_Correlation;

//BEGIN PROCESS OF JOINING ALL NORMALIZATIONS
REC1 := RECORD
  STRING county_fip;
  DECIMAL unemployment_normalization;
  DECIMAL education_normalization;
END;

REC1 TRA1(Unemployment_Normalization Le, Education_Normalization Ri) := TRANSFORM
  SELF.county_fip := Le.county_fip;
  SELF.unemployment_normalization := Le.normalized_unemployment;
  SELF.education_normalization := Ri.normalized_education;
END;

JOIN1 := DEDUP(JOIN(Unemployment_Normalization, Education_Normalization,
  LEFT.county_fip = RIGHT.county_fip,
  TRA1(LEFT,RIGHT),LEFT OUTER,SMART));

//adding poverty
REC2 := RECORD
  STRING county_fip;
  DECIMAL unemployment_normalization;
  DECIMAL education_normalization;
  DECIMAL poverty_normalization;
END;

REC2 TRA2(JOIN1 Le, Poverty_Normalization Ri) := TRANSFORM
  SELF.county_fip := Le.county_fip;
  SELF.unemployment_normalization := Le.unemployment_normalization;
  SELF.education_normalization := Le.education_normalization;
  SELF.poverty_normalization := Ri.normalized_poverty;
END;

JOIN2 := DEDUP(JOIN(JOIN1, Poverty_Normalization,
  LEFT.county_fip = RIGHT.county_fip,
  TRA2(LEFT,RIGHT),LEFT OUTER,SMART));

//adding population 
REC3 := RECORD
  STRING county_fip;
  DECIMAL unemployment_normalization;
  DECIMAL education_normalization;
  DECIMAL poverty_normaliztion;
  DECIMAL population_normalization;
END;

REC3 TRA3(JOIN2 Le, Population_Normalization Ri) := TRANSFORM
  SELF.county_fip := Le.county_fip;
  SELF.unemployment_normalization := Le.unemployment_normalization;
  SELF.education_normalization := Le.education_normalization;
  SELF.poverty_normaliztion := Le.poverty_normalization;
  SELF.population_normalization := Ri.normalized_population;
END;

County_Normalization := DEDUP(JOIN(JOIN2, Population_Normalization,
  LEFT.county_fip = RIGHT.county_fip,
  TRA3(LEFT,RIGHT),LEFT OUTER,SMART));

County_Risk_Record := RECORD
  STRING state;
  STRING county;
  STRING county_fip;
  Decimal9_2 risk;
END;

County_Risk_Record County_Risk_Transform(HMK.City_DS Le, County_Normalization Ri) := TRANSFORM
  SELF.state := Le.state_id;
  SELF.county := Le.county_name;
  SELF.county_fip := Le.county_fips;
  SELF.risk := (Ri.unemployment_normalization * (Unemployment_Correlation/SUM_OF_CORRELATIONS)) + 
               (Ri.education_normalization * (Education_Correlation/SUM_OF_CORRELATIONS)) + 
               (Ri.poverty_normaliztion * (Poverty_Correlation/SUM_OF_CORRELATIONS)) +
               (Ri.population_normalization * (Population_Correlation/SUM_OF_CORRELATIONS));
END;

County_Risk := JOIN(HMK.City_DS, County_Normalization,
                    LEFT.county_fips = RIGHT.county_fip,
                    County_Risk_Transform(LEFT,RIGHT),LEFT OUTER,SMART);
                    
OUTPUT(DEDUP(SORT(County_Risk, -risk)),NAMED('County_Risk'));


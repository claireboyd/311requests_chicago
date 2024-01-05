
-- BATCH VIEW 1: total count of all requests per year/zip code, joined with population data (ckboyd_total_count_pop_summary)

create table ckboyd_total_count_pop_summary(
  zip_code_cleaned string,
  year string,
  total_count int,
  total_population int);

insert overwrite table ckboyd_total_count_pop_summary
  select lh.zip_code_cleaned, lh.year, lh.total_count, rh.total_population
  from (select zip_code_cleaned, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(created_date, 'MM/dd/yyyy hh:mm:ss a'))) as year, count(*) as total_count
    from ckboyd_311requests
    group by zip_code_cleaned, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(created_date, 'MM/dd/yyyy hh:mm:ss a')))
  ) lh
  left join (select geography, year, total_population
    from ckboyd_zippop_csv
  ) rh
  on lh.zip_code_cleaned = rh.geography and lh.year = rh.year
;

-- example
select * from ckboyd_total_count_pop_summary where zip_code_cleaned=="60637";

+--------------------------------------------------+--------------------------------------+---------------------------------------------+--------------------------------------------------+
| ckboyd_total_count_pop_summary.zip_code_cleaned  | ckboyd_total_count_pop_summary.year  | ckboyd_total_count_pop_summary.total_count  | ckboyd_total_count_pop_summary.total_population  |
+--------------------------------------------------+--------------------------------------+---------------------------------------------+--------------------------------------------------+
| 60637                                            | 2018                                 | 709                                         | 47454                                            |
| 60637                                            | 2019                                 | 15314                                       | 47300                                            |
| 60637                                            | 2020                                 | 16366                                       | 46621                                            |
| 60637                                            | 2021                                 | 13826                                       | 49514                                            |
| 60637                                            | 2022                                 | 11322                                       | NULL                                             |
| 60637                                            | 2023                                 | 6742                                        | NULL                                             |
+--------------------------------------------------+--------------------------------------+---------------------------------------------+--------------------------------------------------+

-- BATCH VIEW 2: summary by year/zip code/sr_short_code (ckboyd_requests_freq)
create table ckboyd_requests_freq(
  sr_short_code string, 
  zip_code_cleaned string,
  year string,
  sr_count int);

INSERT overwrite TABLE ckboyd_requests_freq
  SELECT sr_short_code, 
    zip_code_cleaned, 
    YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(created_date, 'MM/dd/yyyy hh:mm:ss a'))) as year, 
    count(1) as sr_count
  FROM ckboyd_311requests
  GROUP BY sr_short_code, zip_code_cleaned, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(created_date, 'MM/dd/yyyy hh:mm:ss a'))) 
;

-- example
select *  from ckboyd_requests_freq where year=="2022" limit 15;

+-------------------------------------+----------------------------------------+----------------------------+--------------------------------+
| ckboyd_requests_freq.sr_short_code  | ckboyd_requests_freq.zip_code_cleaned  | ckboyd_requests_freq.year  | ckboyd_requests_freq.sr_count  |
+-------------------------------------+----------------------------------------+----------------------------+--------------------------------+
| 311IOC                              | 60611                                  | 2022                       | 5                              |
| 311IOC                              | 60614                                  | 2022                       | 23                             |
| 311IOC                              | 60620                                  | 2022                       | 24                             |
| 311IOC                              | 60621                                  | 2022                       | 10                             |
| 311IOC                              | 60640                                  | 2022                       | 9                              |
| 311IOC                              | 60641                                  | 2022                       | 19                             |
| AAD                                 | 60603                                  | 2022                       | 10                             |
| AAD                                 | 60618                                  | 2022                       | 197                            |
| AAD                                 | 60624                                  | 2022                       | 65                             |
| AAD                                 | 60653                                  | 2022                       | 27                             |
| AAD                                 | 60657                                  | 2022                       | 94                             |
| AAE                                 | 60601                                  | 2022                       | 9                              |
| AAE                                 | 60606                                  | 2022                       | 6                              |
| AAE                                 | 60615                                  | 2022                       | 121                            |
| AAE                                 | 60623                                  | 2022                       | 132                            |
+-------------------------------------+----------------------------------------+----------------------------+--------------------------------+
15 rows selected (0.034 seconds)


BATCH VIEW 3: final summary table to use in HBASE (pivoted version of the above to be wide) - ckboyd_requests_summary

create table ckboyd_requests_summary(
  zip_code_cleaned string,
  year string,
  count_311IOC int, 
  count_AAD int, 
  count_AVN int, 
  count_BAM int, 
  count_BBA int, 
  count_BBC int, 
  count_BBD int, 
  count_BPI int, 
  count_CAFE int, 
  count_CIAC int, 
  count_CORNVEND int, 
  count_EAE int, 
  count_EAF int, 
  count_EAQ int, 
  count_ESPC int, 
  count_FAC int, 
  count_FPC int, 
  count_HFF int, 
  count_PBD int, 
  count_PBLDR int, 
  count_PBS int, 
  count_PCB int, 
  count_PCC int, 
  count_PET int, 
  count_PHB int, 
  count_PHF int, 
  count_PSL int, 
  count_QAC int, 
  count_RFC int, 
  count_SCB int, 
  count_SCS int, 
  count_SCT int, 
  count_SCX int, 
  count_SDO int, 
  count_SDP int, 
  count_SDW int, 
  count_SEC int, 
  count_SEL int, 
  count_SFC int, 
  count_SFQ int, 
  count_SGA int, 
  count_SGV int, 
  count_SIE int, 
  count_SRRC int, 
  count_SRRP int, 
  count_SWSNOREM int, 
  count_TNP int, 
  count_VBL int, 
  count_WCA3 int, 
  count_WM3 int, 
  count_AAE int, 
  count_AAF int, 
  count_AAI int, 
  count_BAG int, 
  count_BBK int, 
  count_BUNGALOW int, 
  count_CHECKFOR int, 
  count_CSC int, 
  count_CSF int, 
  count_CSP int, 
  count_CST int, 
  count_DBPC int, 
  count_EAB int, 
  count_EBD int, 
  count_GRAF int, 
  count_HDF int, 
  count_HFB int, 
  count_HOP int, 
  count_INR int, 
  count_JNS int, 
  count_LIQUORCO int, 
  count_LPRC int, 
  count_MWC int, 
  count_NAA int, 
  count_NOSOLCPP int, 
  count_OCC int, 
  count_ODM int, 
  count_PBE int, 
  count_PCD int, 
  count_PCE int, 
  count_PCL int, 
  count_PCL3 int, 
  count_PETCO int, 
  count_RBL int, 
  count_SCC int, 
  count_SCP int, 
  count_SCQ int, 
  count_SDR int, 
  count_SED int, 
  count_SEE int, 
  count_SEF int, 
  count_SFA int, 
  count_SFB int, 
  count_SFD int, 
  count_SFK int, 
  count_SFN int, 
  count_SGG int, 
  count_SGQ int, 
  count_SHVR int, 
  count_SKA int, 
  count_SNPBLBS int, 
  count_WBJ int, 
  count_WBK int, 
  count_WBT int, 
  count_WCA int, 
  count_WCA2 int,
  total_count int,
  total_population int
  );


insert overwrite TABLE ckboyd_requests_summary
select lh.*, rh.total_count, rh.total_population
from (
select
    b.zip_code_cleaned,
    b.year,
    CASE WHEN size(311IOC) == 1 THEN 311IOC[0] ELSE 0 END AS count_311IOC, 
    CASE WHEN size(AAD) == 1 THEN AAD[0] ELSE 0 END AS count_AAD, 
    CASE WHEN size(AVN) == 1 THEN AVN[0] ELSE 0 END AS count_AVN, 
    CASE WHEN size(BAM) == 1 THEN BAM[0] ELSE 0 END AS count_BAM, 
    CASE WHEN size(BBA) == 1 THEN BBA[0] ELSE 0 END AS count_BBA, 
    CASE WHEN size(BBC) == 1 THEN BBC[0] ELSE 0 END AS count_BBC, 
    CASE WHEN size(BBD) == 1 THEN BBD[0] ELSE 0 END AS count_BBD, 
    CASE WHEN size(BPI) == 1 THEN BPI[0] ELSE 0 END AS count_BPI, 
    CASE WHEN size(CAFE) == 1 THEN CAFE[0] ELSE 0 END AS count_CAFE, 
    CASE WHEN size(CIAC) == 1 THEN CIAC[0] ELSE 0 END AS count_CIAC, 
    CASE WHEN size(CORNVEND) == 1 THEN CORNVEND[0] ELSE 0 END AS count_CORNVEND, 
    CASE WHEN size(EAE) == 1 THEN EAE[0] ELSE 0 END AS count_EAE, 
    CASE WHEN size(EAF) == 1 THEN EAF[0] ELSE 0 END AS count_EAF, 
    CASE WHEN size(EAQ) == 1 THEN EAQ[0] ELSE 0 END AS count_EAQ, 
    CASE WHEN size(ESPC) == 1 THEN ESPC[0] ELSE 0 END AS count_ESPC, 
    CASE WHEN size(FAC) == 1 THEN FAC[0] ELSE 0 END AS count_FAC, 
    CASE WHEN size(FPC) == 1 THEN FPC[0] ELSE 0 END AS count_FPC, 
    CASE WHEN size(HFF) == 1 THEN HFF[0] ELSE 0 END AS count_HFF, 
    CASE WHEN size(PBD) == 1 THEN PBD[0] ELSE 0 END AS count_PBD, 
    CASE WHEN size(PBLDR) == 1 THEN PBLDR[0] ELSE 0 END AS count_PBLDR, 
    CASE WHEN size(PBS) == 1 THEN PBS[0] ELSE 0 END AS count_PBS, 
    CASE WHEN size(PCB) == 1 THEN PCB[0] ELSE 0 END AS count_PCB, 
    CASE WHEN size(PCC) == 1 THEN PCC[0] ELSE 0 END AS count_PCC, 
    CASE WHEN size(PET) == 1 THEN PET[0] ELSE 0 END AS count_PET, 
    CASE WHEN size(PHB) == 1 THEN PHB[0] ELSE 0 END AS count_PHB, 
    CASE WHEN size(PHF) == 1 THEN PHF[0] ELSE 0 END AS count_PHF, 
    CASE WHEN size(PSL) == 1 THEN PSL[0] ELSE 0 END AS count_PSL, 
    CASE WHEN size(QAC) == 1 THEN QAC[0] ELSE 0 END AS count_QAC, 
    CASE WHEN size(RFC) == 1 THEN RFC[0] ELSE 0 END AS count_RFC, 
    CASE WHEN size(SCB) == 1 THEN SCB[0] ELSE 0 END AS count_SCB, 
    CASE WHEN size(SCS) == 1 THEN SCS[0] ELSE 0 END AS count_SCS, 
    CASE WHEN size(SCT) == 1 THEN SCT[0] ELSE 0 END AS count_SCT, 
    CASE WHEN size(SCX) == 1 THEN SCX[0] ELSE 0 END AS count_SCX, 
    CASE WHEN size(SDO) == 1 THEN SDO[0] ELSE 0 END AS count_SDO, 
    CASE WHEN size(SDP) == 1 THEN SDP[0] ELSE 0 END AS count_SDP, 
    CASE WHEN size(SDW) == 1 THEN SDW[0] ELSE 0 END AS count_SDW, 
    CASE WHEN size(SEC) == 1 THEN SEC[0] ELSE 0 END AS count_SEC, 
    CASE WHEN size(SEL) == 1 THEN SEL[0] ELSE 0 END AS count_SEL, 
    CASE WHEN size(SFC) == 1 THEN SFC[0] ELSE 0 END AS count_SFC, 
    CASE WHEN size(SFQ) == 1 THEN SFQ[0] ELSE 0 END AS count_SFQ, 
    CASE WHEN size(SGA) == 1 THEN SGA[0] ELSE 0 END AS count_SGA, 
    CASE WHEN size(SGV) == 1 THEN SGV[0] ELSE 0 END AS count_SGV, 
    CASE WHEN size(SIE) == 1 THEN SIE[0] ELSE 0 END AS count_SIE, 
    CASE WHEN size(SRRC) == 1 THEN SRRC[0] ELSE 0 END AS count_SRRC, 
    CASE WHEN size(SRRP) == 1 THEN SRRP[0] ELSE 0 END AS count_SRRP, 
    CASE WHEN size(SWSNOREM) == 1 THEN SWSNOREM[0] ELSE 0 END AS count_SWSNOREM, 
    CASE WHEN size(TNP) == 1 THEN TNP[0] ELSE 0 END AS count_TNP, 
    CASE WHEN size(VBL) == 1 THEN VBL[0] ELSE 0 END AS count_VBL, 
    CASE WHEN size(WCA3) == 1 THEN WCA3[0] ELSE 0 END AS count_WCA3, 
    CASE WHEN size(WM3) == 1 THEN WM3[0] ELSE 0 END AS count_WM3, 
    CASE WHEN size(AAE) == 1 THEN AAE[0] ELSE 0 END AS count_AAE, 
    CASE WHEN size(AAF) == 1 THEN AAF[0] ELSE 0 END AS count_AAF, 
    CASE WHEN size(AAI) == 1 THEN AAI[0] ELSE 0 END AS count_AAI, 
    CASE WHEN size(BAG) == 1 THEN BAG[0] ELSE 0 END AS count_BAG, 
    CASE WHEN size(BBK) == 1 THEN BBK[0] ELSE 0 END AS count_BBK, 
    CASE WHEN size(BUNGALOW) == 1 THEN BUNGALOW[0] ELSE 0 END AS count_BUNGALOW, 
    CASE WHEN size(CHECKFOR) == 1 THEN CHECKFOR[0] ELSE 0 END AS count_CHECKFOR, 
    CASE WHEN size(CSC) == 1 THEN CSC[0] ELSE 0 END AS count_CSC, 
    CASE WHEN size(CSF) == 1 THEN CSF[0] ELSE 0 END AS count_CSF, 
    CASE WHEN size(CSP) == 1 THEN CSP[0] ELSE 0 END AS count_CSP, 
    CASE WHEN size(CST) == 1 THEN CST[0] ELSE 0 END AS count_CST, 
    CASE WHEN size(DBPC) == 1 THEN DBPC[0] ELSE 0 END AS count_DBPC, 
    CASE WHEN size(EAB) == 1 THEN EAB[0] ELSE 0 END AS count_EAB, 
    CASE WHEN size(EBD) == 1 THEN EBD[0] ELSE 0 END AS count_EBD, 
    CASE WHEN size(GRAF) == 1 THEN GRAF[0] ELSE 0 END AS count_GRAF, 
    CASE WHEN size(HDF) == 1 THEN HDF[0] ELSE 0 END AS count_HDF, 
    CASE WHEN size(HFB) == 1 THEN HFB[0] ELSE 0 END AS count_HFB, 
    CASE WHEN size(HOP) == 1 THEN HOP[0] ELSE 0 END AS count_HOP, 
    CASE WHEN size(INR) == 1 THEN INR[0] ELSE 0 END AS count_INR, 
    CASE WHEN size(JNS) == 1 THEN JNS[0] ELSE 0 END AS count_JNS, 
    CASE WHEN size(LIQUORCO) == 1 THEN LIQUORCO[0] ELSE 0 END AS count_LIQUORCO, 
    CASE WHEN size(LPRC) == 1 THEN LPRC[0] ELSE 0 END AS count_LPRC, 
    CASE WHEN size(MWC) == 1 THEN MWC[0] ELSE 0 END AS count_MWC, 
    CASE WHEN size(NAA) == 1 THEN NAA[0] ELSE 0 END AS count_NAA, 
    CASE WHEN size(NOSOLCPP) == 1 THEN NOSOLCPP[0] ELSE 0 END AS count_NOSOLCPP, 
    CASE WHEN size(OCC) == 1 THEN OCC[0] ELSE 0 END AS count_OCC, 
    CASE WHEN size(ODM) == 1 THEN ODM[0] ELSE 0 END AS count_ODM, 
    CASE WHEN size(PBE) == 1 THEN PBE[0] ELSE 0 END AS count_PBE, 
    CASE WHEN size(PCD) == 1 THEN PCD[0] ELSE 0 END AS count_PCD, 
    CASE WHEN size(PCE) == 1 THEN PCE[0] ELSE 0 END AS count_PCE, 
    CASE WHEN size(PCL) == 1 THEN PCL[0] ELSE 0 END AS count_PCL, 
    CASE WHEN size(PCL3) == 1 THEN PCL3[0] ELSE 0 END AS count_PCL3, 
    CASE WHEN size(PETCO) == 1 THEN PETCO[0] ELSE 0 END AS count_PETCO, 
    CASE WHEN size(RBL) == 1 THEN RBL[0] ELSE 0 END AS count_RBL, 
    CASE WHEN size(SCC) == 1 THEN SCC[0] ELSE 0 END AS count_SCC, 
    CASE WHEN size(SCP) == 1 THEN SCP[0] ELSE 0 END AS count_SCP, 
    CASE WHEN size(SCQ) == 1 THEN SCQ[0] ELSE 0 END AS count_SCQ, 
    CASE WHEN size(SDR) == 1 THEN SDR[0] ELSE 0 END AS count_SDR, 
    CASE WHEN size(SED) == 1 THEN SED[0] ELSE 0 END AS count_SED, 
    CASE WHEN size(SEE) == 1 THEN SEE[0] ELSE 0 END AS count_SEE, 
    CASE WHEN size(SEF) == 1 THEN SEF[0] ELSE 0 END AS count_SEF, 
    CASE WHEN size(SFA) == 1 THEN SFA[0] ELSE 0 END AS count_SFA, 
    CASE WHEN size(SFB) == 1 THEN SFB[0] ELSE 0 END AS count_SFB, 
    CASE WHEN size(SFD) == 1 THEN SFD[0] ELSE 0 END AS count_SFD, 
    CASE WHEN size(SFK) == 1 THEN SFK[0] ELSE 0 END AS count_SFK, 
    CASE WHEN size(SFN) == 1 THEN SFN[0] ELSE 0 END AS count_SFN, 
    CASE WHEN size(SGG) == 1 THEN SGG[0] ELSE 0 END AS count_SGG, 
    CASE WHEN size(SGQ) == 1 THEN SGQ[0] ELSE 0 END AS count_SGQ, 
    CASE WHEN size(SHVR) == 1 THEN SHVR[0] ELSE 0 END AS count_SHVR, 
    CASE WHEN size(SKA) == 1 THEN SKA[0] ELSE 0 END AS count_SKA, 
    CASE WHEN size(SNPBLBS) == 1 THEN SNPBLBS[0] ELSE 0 END AS count_SNPBLBS, 
    CASE WHEN size(WBJ) == 1 THEN WBJ[0] ELSE 0 END AS count_WBJ, 
    CASE WHEN size(WBK) == 1 THEN WBK[0] ELSE 0 END AS count_WBK, 
    CASE WHEN size(WBT) == 1 THEN WBT[0] ELSE 0 END AS count_WBT, 
    CASE WHEN size(WCA) == 1 THEN WCA[0] ELSE 0 END AS count_WCA, 
    CASE WHEN size(WCA2) == 1 THEN WCA2[0] ELSE 0 END AS count_WCA2
from 
    (
        select zip_code_cleaned, year,
          collect_list(a.group_map['311IOC']) as 311IOC, 
          collect_list(a.group_map['AAD']) as AAD, 
          collect_list(a.group_map['AVN']) as AVN, 
          collect_list(a.group_map['BAM']) as BAM, 
          collect_list(a.group_map['BBA']) as BBA, 
          collect_list(a.group_map['BBC']) as BBC, 
          collect_list(a.group_map['BBD']) as BBD, 
          collect_list(a.group_map['BPI']) as BPI, 
          collect_list(a.group_map['CAFE']) as CAFE, 
          collect_list(a.group_map['CIAC']) as CIAC, 
          collect_list(a.group_map['CORNVEND']) as CORNVEND, 
          collect_list(a.group_map['EAE']) as EAE, 
          collect_list(a.group_map['EAF']) as EAF, 
          collect_list(a.group_map['EAQ']) as EAQ, 
          collect_list(a.group_map['ESPC']) as ESPC, 
          collect_list(a.group_map['FAC']) as FAC, 
          collect_list(a.group_map['FPC']) as FPC, 
          collect_list(a.group_map['HFF']) as HFF, 
          collect_list(a.group_map['PBD']) as PBD, 
          collect_list(a.group_map['PBLDR']) as PBLDR, 
          collect_list(a.group_map['PBS']) as PBS, 
          collect_list(a.group_map['PCB']) as PCB, 
          collect_list(a.group_map['PCC']) as PCC, 
          collect_list(a.group_map['PET']) as PET, 
          collect_list(a.group_map['PHB']) as PHB, 
          collect_list(a.group_map['PHF']) as PHF, 
          collect_list(a.group_map['PSL']) as PSL, 
          collect_list(a.group_map['QAC']) as QAC, 
          collect_list(a.group_map['RFC']) as RFC, 
          collect_list(a.group_map['SCB']) as SCB, 
          collect_list(a.group_map['SCS']) as SCS, 
          collect_list(a.group_map['SCT']) as SCT, 
          collect_list(a.group_map['SCX']) as SCX, 
          collect_list(a.group_map['SDO']) as SDO, 
          collect_list(a.group_map['SDP']) as SDP, 
          collect_list(a.group_map['SDW']) as SDW, 
          collect_list(a.group_map['SEC']) as SEC, 
          collect_list(a.group_map['SEL']) as SEL, 
          collect_list(a.group_map['SFC']) as SFC, 
          collect_list(a.group_map['SFQ']) as SFQ, 
          collect_list(a.group_map['SGA']) as SGA, 
          collect_list(a.group_map['SGV']) as SGV, 
          collect_list(a.group_map['SIE']) as SIE, 
          collect_list(a.group_map['SRRC']) as SRRC, 
          collect_list(a.group_map['SRRP']) as SRRP, 
          collect_list(a.group_map['SWSNOREM']) as SWSNOREM, 
          collect_list(a.group_map['TNP']) as TNP, 
          collect_list(a.group_map['VBL']) as VBL, 
          collect_list(a.group_map['WCA3']) as WCA3, 
          collect_list(a.group_map['WM3']) as WM3, 
          collect_list(a.group_map['AAE']) as AAE, 
          collect_list(a.group_map['AAF']) as AAF, 
          collect_list(a.group_map['AAI']) as AAI, 
          collect_list(a.group_map['BAG']) as BAG, 
          collect_list(a.group_map['BBK']) as BBK, 
          collect_list(a.group_map['BUNGALOW']) as BUNGALOW, 
          collect_list(a.group_map['CHECKFOR']) as CHECKFOR, 
          collect_list(a.group_map['CSC']) as CSC, 
          collect_list(a.group_map['CSF']) as CSF, 
          collect_list(a.group_map['CSP']) as CSP, 
          collect_list(a.group_map['CST']) as CST, 
          collect_list(a.group_map['DBPC']) as DBPC, 
          collect_list(a.group_map['EAB']) as EAB, 
          collect_list(a.group_map['EBD']) as EBD, 
          collect_list(a.group_map['GRAF']) as GRAF, 
          collect_list(a.group_map['HDF']) as HDF, 
          collect_list(a.group_map['HFB']) as HFB, 
          collect_list(a.group_map['HOP']) as HOP, 
          collect_list(a.group_map['INR']) as INR, 
          collect_list(a.group_map['JNS']) as JNS, 
          collect_list(a.group_map['LIQUORCO']) as LIQUORCO, 
          collect_list(a.group_map['LPRC']) as LPRC, 
          collect_list(a.group_map['MWC']) as MWC, 
          collect_list(a.group_map['NAA']) as NAA, 
          collect_list(a.group_map['NOSOLCPP']) as NOSOLCPP, 
          collect_list(a.group_map['OCC']) as OCC, 
          collect_list(a.group_map['ODM']) as ODM, 
          collect_list(a.group_map['PBE']) as PBE, 
          collect_list(a.group_map['PCD']) as PCD, 
          collect_list(a.group_map['PCE']) as PCE, 
          collect_list(a.group_map['PCL']) as PCL, 
          collect_list(a.group_map['PCL3']) as PCL3, 
          collect_list(a.group_map['PETCO']) as PETCO, 
          collect_list(a.group_map['RBL']) as RBL, 
          collect_list(a.group_map['SCC']) as SCC, 
          collect_list(a.group_map['SCP']) as SCP, 
          collect_list(a.group_map['SCQ']) as SCQ, 
          collect_list(a.group_map['SDR']) as SDR, 
          collect_list(a.group_map['SED']) as SED, 
          collect_list(a.group_map['SEE']) as SEE, 
          collect_list(a.group_map['SEF']) as SEF, 
          collect_list(a.group_map['SFA']) as SFA, 
          collect_list(a.group_map['SFB']) as SFB, 
          collect_list(a.group_map['SFD']) as SFD, 
          collect_list(a.group_map['SFK']) as SFK, 
          collect_list(a.group_map['SFN']) as SFN, 
          collect_list(a.group_map['SGG']) as SGG, 
          collect_list(a.group_map['SGQ']) as SGQ, 
          collect_list(a.group_map['SHVR']) as SHVR, 
          collect_list(a.group_map['SKA']) as SKA, 
          collect_list(a.group_map['SNPBLBS']) as SNPBLBS, 
          collect_list(a.group_map['WBJ']) as WBJ, 
          collect_list(a.group_map['WBK']) as WBK, 
          collect_list(a.group_map['WBT']) as WBT, 
          collect_list(a.group_map['WCA']) as WCA, 
          collect_list(a.group_map['WCA2']) as WCA2
        from (
            select
              zip_code_cleaned,
              year,
              map(sr_short_code,sr_count) as group_map 
            from 
              ckboyd_requests_freq
        ) a
        group by
            a.zip_code_cleaned,
            a.year
    ) b
) lh
left join ckboyd_total_count_pop_summary rh
on lh.zip_code_cleaned = rh.zip_code_cleaned and lh.year = rh.year  
;

-- example
select zip_code_cleaned, year, total_count from ckboyd_requests_summary where zip_code_cleaned == "60637" limit 5;

+-------------------+-------+--------------+
| zip_code_cleaned  | year  | total_count  |
+-------------------+-------+--------------+
| 60637             | 2018  | 709          |
| 60637             | 2020  | 16366        |
| 60637             | 2021  | 13826        |
| 60637             | 2019  | 15314        |
| 60637             | 2022  | 11322        |
+-------------------+-------+--------------+


-- BATCH VIEW 4: crosswalk of sr_short_code to sr_type (ckboyd_request_name_crosswalk)

create table ckboyd_request_name_crosswalk(
  sr_short_code string, 
  sr_type string);

insert overwrite table ckboyd_request_name_crosswalk
select sr_short_code, min(sr_type)
from ckboyd_311requests
group by sr_short_code
;

-- example
select * from ckboyd_request_name_crosswalk limit 5;

+----------------------------------------------+----------------------------------------+
| ckboyd_request_name_crosswalk.sr_short_code  | ckboyd_request_name_crosswalk.sr_type  |
+----------------------------------------------+----------------------------------------+
| 311IOC                                       | 311 INFORMATION ONLY CALL              |
| AAD                                          | Sewer Cave-In Inspection Request       |
| AVN                                          | Aircraft Noise Complaint               |
| BAM                                          | Tobacco - Sale to Minors Complaint     |
| BBA                                          | Building Violation                     |
+----------------------------------------------+----------------------------------------+


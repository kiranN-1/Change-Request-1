---------------------------------
-- BIW_ETL_V.MTRL_VEG_DIM
-- Body Differs
---------------------------------
REPLACE VIEW BIW_ETL_V.MTRL_VEG_DIM AS
/*
Created by : Shanker
Created dt : 08-24-2015
Created for Global customer care, Copied veg_mtrl etl view logic

Updated 9 December 2015 by Susan King
Fixed the join logic to correctly derive PROD_PRECMRCL_NM

Updated 16 December 2015 by Susan King
Added a join to correctly derive secondary variety name
Changed the logic to correctly determine basic material number for both seed and non-seed materials

Updated 2 March 2016 by Erl Codizar
VEG 6084 Removed SEED_PROD_ATTR_VEG as source for OWN_RGN_NM and used MTRL_ATTR instead

Updated 6 June 2016 by Pivy Quinones/Des Gutierrez
For DNA 610 - Add a no leading zero mtrl field to veg_mtrl and remove the trim() function from the universes

Updated 11 August 2016 by Pivy Quinones/Des Gutierrez
DNA 779: Optimization

--Modified on	Modified by		Description
--2/14/2018      cognizant		REQ0583927:Add new fields CREATED_BY_NM,CHNG_BY_NM,CREATED_DT
--06/05/2018     Cognizant              REQ0584345 : Added new field PKG_SIZE_DESC
--12/13/2019   Cognizant 					DEF0587193: Added filter on action type.

Updated By: AMOHA7
Uppdated On: 8/5/2022
DDP-4268 Add field PRDHA from MARA into view VEG.ALL_MTRL_VEG, BIW_ETL_V.MTRL_VEG_DIM

 */

LOCKING ROW FOR ACCESS
SELECT
    CAST(MTRL.MTRL_NBR AS VARCHAR(18)) AS MTRL_NBR
	,MTRL.PROD_HRCHY_CD AS PROD_HIERARCHY		--DDP-4268
    ,CAST(MTRL.MTRL_TYP_CD AS VARCHAR(4)) AS MTRL_TYP_CD
    ,CAST(MTRL_GRP.MTRL_GRP_DESC AS VARCHAR(60)) AS FMLY_NM
    ,CAST(MTRL.MTRL_GRP_CD AS VARCHAR(9)) AS MTRL_GRP_CD
    ,CASE WHEN MTRL_GRP.MTRL_GRP_NM LIKE 'SS %' THEN TRIM (SUBSTR(MTRL_GRP.MTRL_GRP_NM,3,20)) ELSE MTRL_GRP.MTRL_GRP_NM END AS CROP_NM
    --,CASE WHEN cast (SUBSTR(MTRL_GRP_NM,1,2) = 'SS' then 'X' else NULL end ) as CHAR(1))as SEED_STK_IND
    ,COALESCE(VEG_FMLY.FMLY_CD, 'Y0') AS FMLY_CD
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_MATERIAL_PRICING_GROUP'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS MTRL_PRC_GRP_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PRODUCTTYPE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS MTRL_CLSFCTN_TYP_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_COVAR'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PROD_PRECMRCL_NBR

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_VARIETY'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS VRTY_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_VARIETY_NAME_CODE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS VRTY_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SECONDARY'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SCND_NM_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_VARIETY_ABBR'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS VRTY_ABBRV_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_TYPEOFREPRODUCTION'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS RPRDCTN_TYP_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_GENERATION'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS GNRTN_CD

     --   Altered for VEG PDP 2 to pull MTRL.BASIC_MTRL_VAL instead of MTRL.PRDCTN_TXT_VAL to determine a basic material number
    ,CAST(LPAD(MTRL.BASIC_MTRL_VAL,18,'0')  AS VARCHAR(18))AS BASIC_MTRL_NBR


    ,COALESCE(CAST(BASIC.MTRL_DESC AS VARCHAR(40)), '') AS BASIC_MTRL_DESC
    ,CAST(LPAD(MTRL.INDUSTRY_STD_DSC,18,'0') AS VARCHAR(18)) AS SEMI_MTRL_NBR

    ,COALESCE(CAST(SEMI.MTRL_DESC AS VARCHAR(40)), '') AS SEMI_MTRL_DESC

    ,COALESCE(CAST(MAX(MSA.PRC_REF_MTRL_NBR) AS VARCHAR(18)), '') AS PRC_REF_MTRL_NBR

    ,CAST(MTRL.CROSS_PLNT_MTRL_STS_CD AS VARCHAR(2)) AS PHASE_CD
    ,CAST(MTRL.BASE_UOM_CD AS VARCHAR(3)) AS BASE_UOM_CD
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_BRAND'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS BRND_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGE_UOM'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PKG_UOM_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGE_UOM'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(30)), '') AS PKG_UOM_DESC

,COALESCE(CAST(MAX								
								
            (								
                CASE								
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGESIZE'								
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC								
                END								
            ) AS VARCHAR(30)), '') AS PKG_SIZE_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGETYPE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PKG_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGETYPE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PKG_TYP_CD


    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGETYPE'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(100)), '') AS PKG_TYP_DESC


    ,COALESCE(CAST(MAX

            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGESIZE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PKG_SIZE_NBR

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_TREATMENT'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS TRTMNT_CD
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_TREATMENT'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(100)), '') AS TRTMNT_DESC
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_TREATMENT'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_LONG_DESC
                END
            ) AS VARCHAR(750)), '') AS TRTMNT_LNG_DESC
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_CARRIER'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(100)), '') AS CARRIER_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SEEDENHANCEMENT'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SEED_ENHCMNT_CD
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SEEDENHANCEMENT'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(100)), '') AS SEED_ENHCMNT_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_MODIFICATION'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS MOD_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SEEDSIZE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SEED_SIZE_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_MINWEIGHTSIZE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS MIN_WGT_TO_SIZE_RTO

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_NUMBEROFSIZES'
                        THEN MTRL_ATTR.CHAR_VAL_NBR
                END
            ) AS VARCHAR(30)), '') AS NBR_OF_SIZES_CNT
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_POLLINATOR'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PLLNTR_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_POLLINATOR_ABBR'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PLLNTR_ABBRV_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PRODUCTIONENVIRONMENT'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SLLNG_ENV_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PRODUCTIONENVIRONMENT'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(100)), '') AS SLLNG_ENV_DESC
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_CROPLABELCODE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS CROP_CD
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_DIFFICULT_TO_PRODUCE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS DFFCLT_TO_PRODC_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_LABEL'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS LABELING_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREPRINTEDLABEL'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PRE_PRNTD_LBL_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_MATURITY_IDX'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS MTRTY_INDX_NBR

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_BREEDERCODE'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS BREEDR_CD

    ,COALESCE(CAST(MTRL_DESC.MTRL_DESC AS VARCHAR(40)), '') AS SHRT_MTRL_DESC

    ,CAST(MTRL.CHNG_DT AS DATE) AS CHNG_DT
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_BLENDER'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS BLNDR_MTRL_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SUBMARKET'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SUB_MRKT_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SUBSUBMARKET'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SUB_SUB_MRKT_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SUPPLYCHAINOWNER'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS SUPPLY_CHN_OWNR_CD

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PRECOMMNAME'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PRE_CMRCL_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_SHELF'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(5)), '') AS SHLF_LFE_DAYS_CNT

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_ROYALTY'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(1)), '') AS ROYALTY_IND

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREVIOUSNAME'
                        AND MTRL_ATTR.INTRNL_CHAR_CNT = 1
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PREV_NM_1
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREVIOUSNAME'
                        AND MTRL_ATTR.INTRNL_CHAR_CNT = 2
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PREV_NM_2
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREVIOUSNAME'
                        AND MTRL_ATTR.INTRNL_CHAR_CNT = 3
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PREV_NM_3
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREVIOUSNAME'
                        AND MTRL_ATTR.INTRNL_CHAR_CNT = 4
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PREV_NM_4
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PREVIOUSNAME'
                        AND MTRL_ATTR.INTRNL_CHAR_CNT = 5
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS PREV_NM_5
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_GROSSMARGIN'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(10)), '') AS GRSS_MRGN_CD

    ,CAST(MTRL.OLD_MTRL_NBR AS VARCHAR(18)) AS OLD_MTRL_NBR
    ,COALESCE(CAST(CRITICALITY.YY_CRIT AS DECIMAL(10) FORMAT '-(I)9'), '') AS CRTCL_NBR

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_MATERIAL_PRICING_GROUP'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(30)), '') AS MTRL_PRC_GRP_DESC

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_PRODUCTTYPE'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(30)), '') AS MTRL_CLSFCTN_TYP_DESC


    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_GENERATION'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(30)), '') AS GNRTN_NM
    ,CAST((1000 * CAST(BASIC_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_NUMER AS DECIMAL(18, 8))) / CAST(BASIC_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_DENOM AS DECIMAL(18, 8)) AS DECIMAL(18, 3)) AS BASIC_TSW
    ,CAST((1000 * CAST(SEMI_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_NUMER AS DECIMAL(18, 8))) / CAST(SEMI_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_DENOM AS DECIMAL(18, 8)) AS DECIMAL(18, 3)) AS SEMI_TSW
    ,COALESCE(PRNTL.MALE_PRNTL_MTRL_NBR, '') AS MALE_PARNT_MTRL_NBR
    ,COALESCE(PRNTL.MALE_PRNTL_BASIC_MTRL_NBR, '') AS MALE_PARNT_BASIC_MTRL_NBR
    ,COALESCE(PRNTL.MALE_PRNTL_BASIC_VARIETY_NM, '') AS MALE_VRTY_NM
    ,COALESCE(PRNTL.FEMALE_PRNTL_MTRL_NBR, '') AS FMALE_PARNT_MTRL_NBR
    ,COALESCE(PRNTL.FEMALE_PRNTL_BASIC_MTRL_NBR, '') AS FMALE_PARNT_BASIC_MTRL_NBR
    ,COALESCE(PRNTL.FEMALE_PRNTL_BASIC_VARIETY_NM, '') AS FMALE_VRTY_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_CARRIER'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS CARRIER_CD
    ,COALESCE
        (
            CASE
                WHEN MTRL.MTRL_TYP_CD = 'FERT'
                    THEN 'FERT'
                WHEN MTRL.MTRL_TYP_CD = 'HALB'
                    THEN
                        CASE
                            WHEN MAX
                                (
                                    CASE
                                        WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGETYPE'
                                            THEN MTRL_ATTR.CHAR_VAL_CHR
                                    END
                                ) = 'DRTY'
                                THEN 'DRTY'
                            WHEN MAX
                                (
                                    CASE
                                        WHEN MTRL_ATTR.CHAR_CD = 'VS_PACKAGETYPE'
                                            THEN MTRL_ATTR.CHAR_VAL_CHR
                                    END
                                ) = 'SEMI'
                                AND MTRL_DESC.MTRL_DESC LIKE '%100.000.000'
                                THEN 'SEMI-100'
                            WHEN MAX(SEA.SEED_ENHCMNT_CD) IS NOT NULL
                                THEN 'SEMI-PRIME'
                            WHEN MAX(CA.MTRL_APP_CARRIER_TYP_DESC) LIKE '%PEL%'
                                THEN 'SEMI-PELLET'
                            ELSE
                                'SEMI-OTHER'
                        END
            END
            ,''
        ) AS AA_MTRL_CD
    ,MTRL.DEL_IND AS DEL_IND

        -- ADDED METADATA FIELDS AND LOGIC TO SELECT THE APPROPRIATE VALUES

    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_BRAND'
                        THEN MTRL_ATTR.CHAR_VAL_ENG_DESC
                END
            ) AS VARCHAR(30)), '') AS BRND_NM_DESC
    ,MTRL.DIV_CD AS DIV_CD
    ,CASE WHEN SUBSTR(MTRL_GRP.MTRL_GRP_NM,1,2) = 'SS' then 'X' else NULL end as SEED_STK_IND

    -- VEG 6084
    --, SEED_PROD_attr_own_rgn.seed_prod_attr_val as OWN_RGN_NM
    ,COALESCE(CAST(MAX
            (
                CASE
                    WHEN MTRL_ATTR.CHAR_CD = 'VS_OWNINGREGION'
                        THEN MTRL_ATTR.CHAR_VAL_CHR
                END
            ) AS VARCHAR(30)), '') AS OWN_RGN_NM

    -- BEGIN Transformations added or altered for the VEG PDP2 implementation */

    -- Precommercial name was changed so that it is now pulled from the HARVEST precommercial name for a given material's variety.  It was previously populated from VS_PRECOMMNAME.  The change was made per Ben Kleijwegt's direction.
    , SEED_PRD_VEG.SEED_PROD_NM as PROD_PRECMRCL_NM

    ,'N-A' AS DF_MTRL_IND

    , SCNDRY_NM.SCNDRY_VRTY_NM AS SCNDRY_VRTY_NM

    -- END Transformations added or altered for the VEG PDP2 implementation */
    -- DNA 610 ADDED (START)
    , TRIM(LEADING '0' FROM MTRL.MTRL_NBR) AS MTRL_NBR_NOLEAD_ZERO
    , NULLIF(TRIM(LEADING '0' FROM MTRL.BASIC_MTRL_VAL),'') AS BASIC_MTRL_NBR_NOLEAD_ZERO
    , NULLIF(TRIM(LEADING '0' FROM SEMI_MTRL_NBR),'') AS SEMI_MTRL_NBR_NOLEAD_ZERO
    -- DNA 610 ADDED (END)
	,MTRL.CREATED_BY_NM --New field added as part of REQ0583927
	,MTRL.CHNG_BY_NM   --New field added as part of REQ0583927
	,MTRL.CREATED_DT --New field added as part of REQ0583927
    ,MAX(CASE WHEN MTRL.ACTION_TYPE = 'D' THEN 'D' ELSE 'A' END) AS ACTION_TYPE
    ,MIN(MTRL.ROW_INSERT_TIMESTAMP) AS ROW_INSERT_TIMESTAMP
    ,MAX(MTRL.ROW_UPDATE_TIMESTAMP) AS ROW_UPDATE_TIMESTAMP
FROM
        -- TROY: CHANGED FROM BIW TO BIW_BASE_V
        -- VPEDD added filter on action_type(DEF0587193)
        (select 
        
        MTRL_NBR
	   ,PROD_HRCHY_CD 	--DDP-4268
       ,MTRL_TYP_CD                         
       ,MTRL_GRP_CD                        
       ,BASIC_MTRL_VAL                    
       ,INDUSTRY_STD_DSC             
       ,CROSS_PLNT_MTRL_STS_CD
       ,BASE_UOM_CD                        
       ,CHNG_DT                                 
       ,OLD_MTRL_NBR                         
       ,DEL_IND                                   
       ,DIV_CD   
       ,CREATED_BY_NM                   
       ,CHNG_BY_NM                          
       ,CREATED_DT       
       ,PRDCTN_INSPCT_TXT                    
       ,ACTION_TYPE                          
       ,ROW_INSERT_TIMESTAMP   
       ,ROW_UPDATE_TIMESTAMP  
           from BIW_BASE_V.MTRL where action_type<>'D') AS MTRL
LEFT OUTER JOIN
        BIW.MTRL_ATTR AS MTRL_ATTR
            ON MTRL.MTRL_NBR = MTRL_ATTR.MTRL_NBR
INNER JOIN
        BIW.MTRL_GRP AS MTRL_GRP
            ON MTRL_GRP.MTRL_GRP_CD = MTRL.MTRL_GRP_CD
LEFT OUTER JOIN
        BIW.FMLY_MON_VEG AS VEG_FMLY
            ON CAST(MTRL_GRP.MTRL_GRP_DESC AS VARCHAR(60)) = VEG_FMLY.FMLY_NM
LEFT OUTER JOIN
        BIW.MTRL_DESC AS MTRL_DESC
            ON MTRL.MTRL_NBR = MTRL_DESC.MTRL_NBR
            AND MTRL_DESC.LANGUAGE_CD = 'E'
LEFT OUTER JOIN
        BIW.MTRL_DESC AS BASIC
            ON LPAD(MTRL.PRDCTN_INSPCT_TXT, 18, '0') = BASIC.MTRL_NBR
                AND BASIC.LANGUAGE_CD = 'E'
LEFT OUTER JOIN
        BIW.MTRL_DESC AS SEMI
            ON LPAD(MTRL.INDUSTRY_STD_DSC, 18, '0') = SEMI.MTRL_NBR
            AND SEMI.LANGUAGE_CD = 'E'
LEFT OUTER JOIN
        SAP_ECC.YPP_CRITICALITY  AS CRITICALITY
            ON LPAD(MTRL.PRDCTN_INSPCT_TXT, 18, '0') = CRITICALITY.YY_BASIC
LEFT OUTER JOIN
        BIW.MTRL_SLS_AREA AS MSA
            ON MTRL.MTRL_NBR = MSA.MTRL_NBR
            AND MSA.PRC_REF_MTRL_NBR <> ' '
LEFT JOIN
        BIW.MTRL_UOM AS SEMI_MTRL_UOM
            ON LPAD(MTRL.INDUSTRY_STD_DSC, 18, '0') = SEMI_MTRL_UOM.MTRL_NBR
--            AND SEMI_MTRL_UOM.ACTION_TYP <> 'D' -- DNA 779 (DELETED)
            AND SEMI_MTRL_UOM.ALT_UOM_CD = 'MK'
LEFT JOIN
        BIW.MTRL_UOM AS BASIC_MTRL_UOM
            ON LPAD(MTRL.PRDCTN_INSPCT_TXT, 18, '0') = BASIC_MTRL_UOM.MTRL_NBR
--            AND BASIC_MTRL_UOM.ACTION_TYP <> 'D' -- DNA 779 (DELETED)
            AND BASIC_MTRL_UOM.ALT_UOM_CD = 'MK'
 LEFT OUTER JOIN
        BIW.VEG_MTRL_PRNTL_MTRL PRNTL
            ON MTRL.MTRL_NBR = PRNTL.MTRL_NBR
/* VEG 6084
LEFT OUTER JOIN
        BIW.SEED_PROD_ATTR_VEG SEED_PROD_attr_own_rgn
            ON  SEED_PROD_attr_own_rgn.SEED_PROD_SRC_SYS_CD='Harvest'
            and SEED_PROD_attr_own_rgn.SEED_PROD_ATTR_NM='Owning Region Name'
            AND SEED_PROD_attr_own_rgn.SEED_PROD_CD = MTRL_ATTR.CHAR_CD
*/
LEFT OUTER JOIN
        BIW.SEED_ENHCMNT SEA
        -- DNA 779 MODIFIED (START)
          /*  ON SEA.SEED_ENHCMNT_CD =
                (
                    CASE
                        WHEN MTRL_ATTR.CHAR_CD = 'VS_SEEDENHANCEMENT'
                            THEN MTRL_ATTR.CHAR_VAL_CHR
                    END
                )
                AND SEA.SEED_ENHCMNT_TYP_DESC LIKE '%PRIM%' */
            ON SEA.SEED_ENHCMNT_CD = MTRL_ATTR.CHAR_VAL_CHR
               AND SEA.SEED_ENHCMNT_TYP_DESC LIKE '%PRIM%'
               AND MTRL_ATTR.CHAR_CD = 'VS_SEEDENHANCEMENT'
        -- -- DNA 779 MODIFIED (END)

-- New join introduced to correctly derive product precommercial name based on the basic material's covar id or precom number
LEFT OUTER JOIN
        BIW.MTRL_ATTR AS BASIC_ATTR
            ON  LPAD(MTRL.PRDCTN_INSPCT_TXT, 18, '0') = BASIC_ATTR.MTRL_NBR
            AND BASIC_ATTR.CHAR_CD= 'VS_COVAR'

-- New join introduced to pull secondary variety names from the BIW_TEMP_T strcuture populated recursively from YSEMSECVARIETY
LEFT OUTER JOIN
        BIW_TEMP_T.MTRL_VEG_SCNDRY_VRTY_NM AS SCNDRY_NM
            ON BASIC_ATTR.CHAR_VAL_CHR = SCNDRY_NM.PROD_PRECMRCL_NBR
            AND BASIC_ATTR.CHAR_CD= 'VS_COVAR'

LEFT OUTER JOIN
        BIW.SEED_PROD_VEG SEED_PRD_VEG
            ON SEED_PRD_VEG.SEED_PROD_CD = BASIC_ATTR.CHAR_VAL_CHR
            and SEED_PRD_VEG.SEED_PROD_SRC_SYS_CD='Harvest'
LEFT OUTER JOIN
        BIW.MTRL_APP_CARRIER CA
        -- DNA 779 MODIFIED (START)
          /*ON CA.MTRL_APP_CARRIER_CD =
                (
                    CASE
                        WHEN MTRL_ATTR.CHAR_CD = 'VS_CARRIER'
                            THEN MTRL_ATTR.CHAR_VAL_CHR
                    END ) */
            ON CA.MTRL_APP_CARRIER_CD = MTRL_ATTR.CHAR_VAL_CHR
               AND MTRL_ATTR.CHAR_CD = 'VS_CARRIER'
        -- DNA 779 MODIFIED (START)

WHERE MTRL_GRP.LANGUAGE_CD = 'E'
AND MTRL.DIV_CD = '27'

GROUP BY
    MTRL.MTRL_NBR
	,MTRL.PROD_HRCHY_CD	--DDP-4268
    ,MTRL.MTRL_TYP_CD
    ,CROP_NM
    ,SEED_STK_IND
    ,MTRL.MTRL_GRP_CD
    ,MTRL_GRP.MTRL_GRP_DESC
    ,MTRL.BASIC_MTRL_VAL
    ,MTRL.INDUSTRY_STD_DSC
    ,MTRL.CROSS_PLNT_MTRL_STS_CD
    ,MTRL.BASE_UOM_CD
    ,MTRL_DESC.MTRL_DESC
    ,MTRL.CHNG_DT
    ,BASIC.MTRL_DESC
    ,SEMI.MTRL_DESC
    ,MTRL.OLD_MTRL_NBR
    ,CRITICALITY.YY_CRIT
    ,PRNTL.MALE_PRNTL_MTRL_NBR
    ,PRNTL.MALE_PRNTL_BASIC_MTRL_NBR
    ,PRNTL.MALE_PRNTL_BASIC_VARIETY_NM
    ,PRNTL.FEMALE_PRNTL_MTRL_NBR
    ,PRNTL.FEMALE_PRNTL_BASIC_MTRL_NBR
    ,PRNTL.FEMALE_PRNTL_BASIC_VARIETY_NM
    ,SEMI_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_NUMER
    ,SEMI_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_DENOM
    ,BASIC_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_NUMER
    ,BASIC_MTRL_UOM.ALT_UNIT_TO_BASE_UNIT_DENOM
    ,MTRL.DEL_IND
    ,VEG_FMLY.FMLY_CD
    ,MTRL.DIV_CD
    --,SEED_PROD_attr_own_rgn.seed_prod_attr_val
    ,SEED_PRD_VEG.SEED_PROD_NM
    ,SCNDRY_NM.SCNDRY_VRTY_NM
	,MTRL.CREATED_BY_NM
	,MTRL.CHNG_BY_NM
	,MTRL.CREATED_DT;
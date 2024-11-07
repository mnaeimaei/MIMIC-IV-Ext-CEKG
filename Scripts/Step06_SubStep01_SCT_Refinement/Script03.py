import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

###################################
################################### Go to SQL \ Query 10_SCT
###################################
# Perform a query.
query1 = f'''            





################################################################################
#query:  2

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_2`   AS
        SELECT DISTINCT
        a.s0, a.s1,
            b.destinationId as s2,
            From `snomed-ct-ml4219.u1.TTTT_1`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s1=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 
            ;



#query:  3

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_3`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2,
            b.destinationId as s3,
            From `snomed-ct-ml4219.u1.TTTT_2`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s2=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 
            ;



#query:  4

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_4`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3,
            b.destinationId as s4,
            From `snomed-ct-ml4219.u1.TTTT_3`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s3=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 
            ;



#query:  5

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_5`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4,
            b.destinationId as s5,
            From `snomed-ct-ml4219.u1.TTTT_4`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s4=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 
            ;



#query:  6

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_6`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5,
            b.destinationId as s6,
            From `snomed-ct-ml4219.u1.TTTT_5`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s5=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 
            ;



#query:  7

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_7`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6,
            b.destinationId as s7,
            From `snomed-ct-ml4219.u1.TTTT_6`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s6=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 
            ;



#query:  8

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_8`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7,
            b.destinationId as s8,
            From `snomed-ct-ml4219.u1.TTTT_7`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s7=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 
            ;



#query:  9

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_9`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8,
            b.destinationId as s9,
            From `snomed-ct-ml4219.u1.TTTT_8`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s8=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 
            ;



#query:  10

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_10`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9,
            b.destinationId as s10,
            From `snomed-ct-ml4219.u1.TTTT_9`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s9=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 
            ;



#query:  11

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_11`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10,
            b.destinationId as s11,
            From `snomed-ct-ml4219.u1.TTTT_10`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s10=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 
            ;



#query:  12

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_12`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11,
            b.destinationId as s12,
            From `snomed-ct-ml4219.u1.TTTT_11`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s11=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 
            ;



#query:  13

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_13`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12,
            b.destinationId as s13,
            From `snomed-ct-ml4219.u1.TTTT_12`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s12=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 
            ;



#query:  14

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_14`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13,
            b.destinationId as s14,
            From `snomed-ct-ml4219.u1.TTTT_13`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s13=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 
            ;



#query:  15

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_15`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14,
            b.destinationId as s15,
            From `snomed-ct-ml4219.u1.TTTT_14`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s14=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 
            ;



#query:  16

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_16`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15,
            b.destinationId as s16,
            From `snomed-ct-ml4219.u1.TTTT_15`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s15=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 
            ;



#query:  17

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_17`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16,
            b.destinationId as s17,
            From `snomed-ct-ml4219.u1.TTTT_16`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s16=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 
            ;



#query:  18

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_18`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17,
            b.destinationId as s18,
            From `snomed-ct-ml4219.u1.TTTT_17`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s17=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 
            ;



#query:  19

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_19`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18,
            b.destinationId as s19,
            From `snomed-ct-ml4219.u1.TTTT_18`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s18=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 
            ;



#query:  20

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_20`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19,
            b.destinationId as s20,
            From `snomed-ct-ml4219.u1.TTTT_19`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s19=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 
            ;



#query:  21

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_21`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20,
            b.destinationId as s21,
            From `snomed-ct-ml4219.u1.TTTT_20`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s20=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 
            ;



#query:  22

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_22`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21,
            b.destinationId as s22,
            From `snomed-ct-ml4219.u1.TTTT_21`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s21=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 
            ;



#query:  23

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_23`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22,
            b.destinationId as s23,
            From `snomed-ct-ml4219.u1.TTTT_22`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s22=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 
            ;



#query:  24

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_24`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23,
            b.destinationId as s24,
            From `snomed-ct-ml4219.u1.TTTT_23`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s23=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 
            ;



#query:  25

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_25`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24,
            b.destinationId as s25,
            From `snomed-ct-ml4219.u1.TTTT_24`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s24=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 
            ;



#query:  26

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_26`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25,
            b.destinationId as s26,
            From `snomed-ct-ml4219.u1.TTTT_25`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s25=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 
            ;



#query:  27

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_27`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25, a.s26,
            b.destinationId as s27,
            From `snomed-ct-ml4219.u1.TTTT_26`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s26=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 AND b.destinationId <> s26 
            ;



#query:  28

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_28`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25, a.s26, a.s27,
            b.destinationId as s28,
            From `snomed-ct-ml4219.u1.TTTT_27`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s27=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 AND b.destinationId <> s26 AND b.destinationId <> s27 
            ;



#query:  29

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_29`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25, a.s26, a.s27, a.s28,
            b.destinationId as s29,
            From `snomed-ct-ml4219.u1.TTTT_28`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s28=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 AND b.destinationId <> s26 AND b.destinationId <> s27 AND b.destinationId <> s28 
            ;



#query:  30

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_30`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25, a.s26, a.s27, a.s28, a.s29,
            b.destinationId as s30,
            From `snomed-ct-ml4219.u1.TTTT_29`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s29=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 AND b.destinationId <> s26 AND b.destinationId <> s27 AND b.destinationId <> s28 AND b.destinationId <> s29 
            ;



#query:  31

        CREATE TABLE  `snomed-ct-ml4219.u1.TTTT_31`   AS
        SELECT DISTINCT
        a.s0, a.s1, a.s2, a.s3, a.s4, a.s5, a.s6, a.s7, a.s8, a.s9, a.s10, a.s11, a.s12, a.s13, a.s14, a.s15, a.s16, a.s17, a.s18, a.s19, a.s20, a.s21, a.s22, a.s23, a.s24, a.s25, a.s26, a.s27, a.s28, a.s29, a.s30,
            b.destinationId as s31,
            From `snomed-ct-ml4219.u1.TTTT_30`   as a
            LEFT JOIN `snomed-ct-ml4219.K05_SCT.REL`   as b
            ON
            a.s30=b.sourceId 
            AND 
            b.destinationId <> s0 AND b.destinationId <> s1 AND b.destinationId <> s2 AND b.destinationId <> s3 AND b.destinationId <> s4 AND b.destinationId <> s5 AND b.destinationId <> s6 AND b.destinationId <> s7 AND b.destinationId <> s8 AND b.destinationId <> s9 AND b.destinationId <> s10 AND b.destinationId <> s11 AND b.destinationId <> s12 AND b.destinationId <> s13 AND b.destinationId <> s14 AND b.destinationId <> s15 AND b.destinationId <> s16 AND b.destinationId <> s17 AND b.destinationId <> s18 AND b.destinationId <> s19 AND b.destinationId <> s20 AND b.destinationId <> s21 AND b.destinationId <> s22 AND b.destinationId <> s23 AND b.destinationId <> s24 AND b.destinationId <> s25 AND b.destinationId <> s26 AND b.destinationId <> s27 AND b.destinationId <> s28 AND b.destinationId <> s29 AND b.destinationId <> s30 
            ;



################################################################################
        CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperA`   AS
        SELECT DISTINCT * FROM `snomed-ct-ml4219.u1.TTTT_31` ;








'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)

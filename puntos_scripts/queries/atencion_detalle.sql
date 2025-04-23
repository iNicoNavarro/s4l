SELECT
    f.ATTID,
    f.ATTFIN01 AS FECHA_ATENCION,
    p.PACID,
    p.PACDOC,
    p.FIRST_NAME,
    p.SECOND_NAME,
    p.LAST_NAME,
    p.PACCITY,
    p.PACINGDAT,
    s.SPENAME,
    g.SPEGRP_NAME,
    a.STATUS_DESC AS ESTADO_ASIGNACION,
    ass.ASSISTED AS ASISTENCIA
FROM 
    fact_attention f
JOIN 
    dim_patient p 
    ON f.PACID = p.PACID
JOIN 
    dim_speciality s 
    ON f.SPEID = s.SPEID
JOIN 
    dim_speciality_group g 
    ON s.SPEGRP_ID = g.SPEGRP_ID
JOIN 
    dim_assignment_status a 
    ON f.ASSIGNMENT_STATUS_ID = a.ASSIGNMENT_STATUS_ID
JOIN 
    dim_assistance_status ass 
    ON f.ASSISTANCE_STATUS_ID = ass.ASSISTANCE_STATUS_ID;
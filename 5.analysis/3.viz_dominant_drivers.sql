-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
  HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers_top_10
AS
  SELECT * FROM v_dominant_drivers
  WHERE driver_rank <= 10

-- COMMAND ----------

SELECT * FROM v_dominant_drivers_top_10

-- COMMAND ----------

/* 
----- solucion del curso con WHERE:
SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC
*/
----- mi solucion con INNER JOIN: mas performante?
SELECT  race_year,
        race_res.driver_name, 
        COUNT(1) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results race_res
INNER JOIN v_dominant_drivers_top_10 top10_dom_driv ON race_res.driver_name = top10_dom_driv.driver_name
GROUP BY race_year, race_res.driver_name
ORDER BY race_year, avg_points DESC
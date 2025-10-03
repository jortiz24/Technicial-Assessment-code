-- this might be very inefficient but first query below sets up a table ordering userids and timestamps to identify sessions per userid and when a user started a new session

CREATE TABLE collections AS(
SELECT user_id,event_timestamp,event_type, CASE WHEN user_id = LAG(user_id) OVER(ORDER BY user_id,event_timestamp ASC) THEN TIMESTAMPDIFF( MINUTE, LAG(event_timestamp) OVER(ORDER BY user_id,event_timestamp ASC),event_timestamp) ELSE NULL END as minutes 
FROM LogTable);


-- Query below adds a column that tracks everytime a new session is created, in newsession a 1 represents a newsession and 0 represents the same session

CREATE TABLE tracksession AS (SELECT user_id,event_timestamp,event_type,minutes,CASE WHEN minutes > 30 or minutes is NULL THEN 1 ELSE 0 END as newsession
FROM collections
);


-- Query below this answers part 1 and displays table 1 after sessionIds are assigned based on newsession values
SELECT user_id,event_timestamp,event_type,minutes,  sum(newsession) OVER( ORDER BY user_id,event_timestamp) AS sessionIds
FROM tracksession;


-- Last query below answers part 2 and identifies the sessions and how many clicks are in the sessions represented in table 2 (bottom table). 1 in event_type is used to represent click event and 0 represents page view
WITH finaltable AS(SELECT user_id,event_timestamp,minutes,event_type,  sum(newsession) OVER( ORDER BY user_id,event_timestamp) AS sessionIds
FROM tracksession)



SELECT sessionIds, COUNT(event_type) AS sessionclicks
FROM finaltable
WHERE event_type = 1
GROUP BY sessionIds;

-- I attempted to add a screenshot to the table with results and simiulated data but could only add one file.
-- I will use a link here for reference instead. https://dbfiddle.uk/7G0A6VMl
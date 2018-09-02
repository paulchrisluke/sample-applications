DROP VIEW public.game_score CASCADE;
DROP VIEW public.offensive_yds CASCADE;
DROP VIEW public.defensive_pat_succeeds CASCADE;
DROP VIEW public.espn_player_points_subtotal CASCADE;
DROP VIEW public.espn_defense_points_subtotal CASCADE;
DROP VIEW public.yahoo_player_points_subtotal CASCADE;
DROP VIEW public.yahoo_defense_points_subtotal CASCADE;

CREATE VIEW public.game_score AS(
SELECT
	a.gsis_id,
	a.team,
	CASE
		WHEN a.team=b.home_team THEN home_score
		WHEN a.team=away_team THEN away_score
		ELSE NULL END AS team_score,
	CASE
		WHEN a.team=b.away_team THEN home_score
		WHEN a.team=home_team THEN away_score
		ELSE NULL END AS oppo_score
FROM public.play_player a
LEFT JOIN public.game b ON (b.home_team=a.team OR b.away_team=a.team) AND a.gsis_id=b.gsis_id
GROUP BY a.gsis_id, a.team, team_score, oppo_score);

CREATE VIEW public.offensive_yds AS(
SELECT
	gsis_id,
	(SUM(COALESCE(passing_yds)) + SUM(COALESCE(rushing_yds)) + SUM(COALESCE(receiving_yds)) + SUM(COALESCE(fumbles_rec_yds))) AS offensive_yds
FROM public.play_player
GROUP BY gsis_id);

CREATE VIEW public.defensive_pat_succeeds AS(
SELECT
	a.gsis_id,
	CASE WHEN a.pos_team = b.home_team THEN b.away_team
		WHEN a.pos_team = b.away_team THEN b.home_team
		ELSE Null END AS team
FROM play a
LEFT JOIN game b ON a.gsis_id = b.gsis_id
WHERE a.description ~ 'DEFENSIVE TWO-POINT ATTEMPT' AND a.description ~ 'ATTEMPT SUCCEEDS');
																								  
CREATE VIEW public.espn_player_points_subtotal AS(
SELECT
	player_id,
	gsis_id,
	team,
	SUM(passing_yds) / 25 AS passing_yds,
	SUM(passing_tds) * 4 AS passing_tds,
	SUM(passing_int) * (-2) AS passing_int,
	SUM(passing_twoptm) * 2 AS passing_twoptm,
	SUM(rushing_yds) / 10 AS rushing_yds,
	SUM(rushing_tds) * 6 AS rushing_tds,
	SUM(rushing_twoptm) * 2 AS rushing_twoptm,
	SUM(receiving_yds) / 10 AS receiving_yds,
	SUM(receiving_tds) * 6 AS receiving_tds,
	SUM(receiving_twoptm) * 2 AS receiving_twoptm,
	SUM(kickret_tds) * 6 AS kickret_tds,
	SUM(puntret_tds) * 6 AS puntret_tds,
	SUM(fumbles_rec_tds) * 6 AS fumbles_rec_tds,
	SUM(fumbles_lost) * (-2) AS fumbles_lost,
	SUM(defense_int_tds) * 6 AS defense_int_tds,
	SUM(defense_frec_tds) * 6 AS defense_frec_tds,
	SUM(defense_misc_tds) * 6 AS defense_misc_tds,
	SUM(kicking_xpmade) AS kicking_xpmade,
	SUM(kicking_fgmissed) * (-1) AS kicking_fgmissed,
	SUM(kicking_fgm * CASE
						WHEN kicking_fgm_yds < 40 THEN 3
						WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN 4
						WHEN kicking_fgm_yds >= 50 THEN 5 
						ELSE NULL END) AS kicking_fgm,
	(SUM(defense_puntblk) + SUM(kicking_xpb) + SUM(defense_fgblk)) * 2 AS defense_blk,
	SUM(defense_int) * 2 AS defense_int,
	SUM(defense_frec) * 2 AS defense_frec,
	SUM(defense_safe) * 2 AS defense_safe
FROM public.play_player
GROUP BY player_id, gsis_id, team);
	 
CREATE VIEW public.yahoo_player_points_subtotal AS(
SELECT
	player_id,
	gsis_id,
	team,
	SUM(passing_yds) / 25 AS passing_yds,
	SUM(passing_tds) * 4 AS passing_tds,
	SUM(passing_int) * (-1) AS passing_int,
	SUM(rushing_yds) / 10 AS rushing_yds,
	SUM(rushing_tds) * 6 AS rushing_tds,
	SUM(receiving_rec) / 2 AS receiving_rec,
	SUM(receiving_yds) / 10 AS receiving_yds,
	SUM(receiving_tds) * 6 AS receiving_tds,
	SUM(kickret_tds) * 6 AS kickret_tds,
	SUM(puntret_tds) * 6 AS puntret_tds,
	SUM(passing_twoptm) * 2 AS passing_twoptm,
	SUM(rushing_twoptm) * 2 AS rushing_twoptm,
	SUM(receiving_twoptm) * 2 AS receiving_twoptm,
	SUM(fumbles_lost) * (-2) AS fumbles_lost,
	SUM(fumbles_rec_tds) * 6 AS fumbles_rec_tds,
	SUM(kicking_fgm * CASE
						WHEN kicking_fgm_yds < 20 THEN 3
						WHEN kicking_fgm_yds >= 20 AND kicking_fgm_yds < 30 THEN 3
						WHEN kicking_fgm_yds >= 30 AND kicking_fgm_yds < 40 THEN 3
						WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN 4
						WHEN kicking_fgm_yds >= 50 THEN 5 
						ELSE NULL END) AS kicking_fgm,
	SUM(kicking_xpmade) AS kicking_xpmade
FROM public.play_player
GROUP BY player_id, gsis_id, team);
	 
CREATE VIEW public.espn_player_points_total AS(
SELECT
	player_id,
	gsis_id,
	team,
	(passing_yds + passing_tds + passing_int + passing_twoptm
	 + rushing_yds + rushing_tds + rushing_twoptm
	 + receiving_yds + receiving_tds + receiving_twoptm
	 + kickret_tds + puntret_tds
	 + fumbles_rec_tds + fumbles_lost
	 + defense_int_tds + defense_frec_tds + defense_misc_tds
	 + kicking_xpmade + kicking_fgmissed + kicking_fgm
	 + defense_blk + defense_int + defense_frec + defense_safe) AS fantasy_points
FROM public.espn_player_points_subtotal);
	 
CREATE VIEW public.yahoo_player_points_total AS(
SELECT
	player_id,
	gsis_id,
	team,
	(passing_yds + passing_tds + passing_int
	 + rushing_yds + rushing_tds
	 + receiving_rec + receiving_yds + receiving_tds
	 + kickret_tds + puntret_tds
	 + passing_twoptm + rushing_twoptm + receiving_twoptm
	 + fumbles_lost + fumbles_rec_tds
	 + kicking_fgm + kicking_xpmade) AS fantasy_points
FROM public.yahoo_player_points_subtotal);
	 
CREATE VIEW public.espn_defense_points_subtotal AS(
SELECT
	gsis_id,
	SUM(COALESCE(defense_sk, 0)) AS defense_sk,
	SUM(COALESCE(defense_int_tds, 0)) * 6 AS defense_int_tds,
	SUM(COALESCE(defense_frec_tds, 0)) * 6 AS defense_frec_tds,
	SUM(COALESCE(kickret_tds, 0)) * 6 AS kickret_tds,
	SUM(COALESCE(puntret_tds, 0)) * 6 AS puntret_tds,
	SUM(COALESCE(defense_misc_tds, 0)) * 6 AS defense_misc_tds,
	(SUM(COALESCE(defense_puntblk, 0)) + SUM(COALESCE(kicking_xpb, 0)) + SUM(COALESCE(defense_fgblk, 0))) * 2 AS defense_blk,
	SUM(COALESCE(defense_int, 0)) * 2 AS defense_int,
	SUM(COALESCE(defense_frec, 0)) * 2 AS defense_frec,
	SUM(COALESCE(defense_safe, 0)) * 2 AS defense_safe
FROM public.agg_play
GROUP BY gsis_id);
		
CREATE VIEW public.yahoo_defense_points_subtotal AS(
SELECT
	gsis_id,
	SUM(COALESCE(defense_sk, 0)) AS defense_sk,
	SUM(COALESCE(defense_int, 0)) * 2 AS defense_int,
	SUM(COALESCE(defense_frec, 0)) * 2 AS defense_frec,
	SUM(COALESCE(defense_int_tds, 0)) * 6 AS defense_int_tds,
	SUM(COALESCE(defense_frec_tds, 0)) * 6 AS defense_frec_tds,
	SUM(COALESCE(defense_misc_tds, 0)) * 6 AS defense_misc_tds,
	SUM(COALESCE(defense_safe, 0)) * 2 AS defense_safe,
	(SUM(COALESCE(defense_puntblk, 0)) + SUM(COALESCE(kicking_xpb, 0)) + SUM(COALESCE(defense_fgblk, 0))) * 2 AS defense_blk,
	SUM(COALESCE(kickret_tds, 0)) * 6 AS kickret_tds,
	SUM(COALESCE(puntret_tds, 0)) * 6 AS puntret_tds
FROM public.agg_play
GROUP BY gsis_id);
		
CREATE VIEW public.espn_defense_points_subset AS(
SELECT
	a.gsis_id,
	a.team,
	CASE WHEN a.oppo_score = 0 THEN 5
		WHEN a.oppo_score > 0 AND a.oppo_score < 7 THEN 4
		WHEN a.oppo_score > 6 AND a.oppo_score < 14 THEN 3
		WHEN a.oppo_score > 13 AND a.oppo_score < 18 THEN 1
		WHEN a.oppo_score > 27 AND a.oppo_score < 35 THEN -1
		WHEN a.oppo_score > 34 AND a.oppo_score < 46 THEN -3
		WHEN a.oppo_score > 45 THEN -5
		ELSE 0 END AS defense_points_pa,
	CASE WHEN b.offensive_yds < 100 THEN 5
		WHEN b.offensive_yds >= 100 AND b.offensive_yds < 200 THEN 3
		WHEN b.offensive_yds >= 200 AND b.offensive_yds < 300 THEN 2
		WHEN b.offensive_yds >= 350 AND b.offensive_yds < 400 THEN -1
		WHEN b.offensive_yds >= 400 AND b.offensive_yds < 450 THEN -3
		WHEN b.offensive_yds >= 450 AND b.offensive_yds < 500 THEN -5
		WHEN b.offensive_yds >= 500 AND b.offensive_yds < 550 THEN -6
		WHEN b.offensive_yds >= 550 THEN -7
		ELSE 0 END AS defense_points_ya
FROM public.game_score a
LEFT JOIN public.offensive_yds b ON a.gsis_id = b.gsis_id);

CREATE VIEW public.yahoo_defense_points_subset AS(
SELECT
	a.gsis_id,
	a.team,
	CASE WHEN a.oppo_score = 0 THEN 10
		WHEN a.oppo_score > 0 AND a.oppo_score < 7 THEN 7
		WHEN a.oppo_score > 6 AND a.oppo_score < 14 THEN 4
		WHEN a.oppo_score > 13 AND a.oppo_score < 21 THEN 1
		WHEN a.oppo_score > 20 AND a.oppo_score < 28 THEN 0
		WHEN a.oppo_score > 27 AND a.oppo_score < 35 THEN -1
		WHEN a.oppo_score > 34 THEN -4
		ELSE 0 END AS defense_points_pa,
	CASE WHEN a.team = b.team THEN 2
		ELSE 0 END AS defense_points_xp
FROM public.game_score a
LEFT JOIN public.defensive_pat_succeeds b ON a.gsis_id = b.gsis_id);
																								  
CREATE VIEW public.espn_defense_points_total AS(
SELECT
	b.team || ' D/ST' AS player_id,
	a.gsis_id,
	b.team,
	(a.defense_sk + a.defense_int_tds + a.defense_frec_tds
	 + a.kickret_tds + a.puntret_tds + a.defense_misc_tds
	 + a.defense_blk + a.defense_int + a.defense_frec + a.defense_safe
	 + b.defense_points_pa + b.defense_points_ya) AS fantasy_points
FROM public.espn_defense_points_subtotal a
LEFT JOIN public.espn_defense_points_subset b ON a.gsis_id=b.gsis_id);
	
CREATE VIEW public.yahoo_defense_points_total AS(
SELECT
	b.team || ' D/ST' AS player_id,
	a.gsis_id,
	b.team,
	(a.defense_sk + a.defense_int + a.defense_frec
	 + a.defense_int_tds + a.defense_frec_tds + a.defense_misc_tds
	 + a.defense_safe + a.defense_blk + a.kickret_tds + a.puntret_tds
	 + b.defense_points_pa + b.defense_points_xp) AS fantasy_points
FROM public.yahoo_defense_points_subtotal a
LEFT JOIN public.yahoo_defense_points_subset b ON a.gsis_id=b.gsis_id);
		
CREATE VIEW public.fantasy_football_points_total AS(
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points
FROM public.espn_player_points_total
UNION
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points
FROM public.espn_defense_points_total);
		

DROP VIEW public.game_score CASCADE;
DROP VIEW public.offensive_yds CASCADE;
DROP VIEW public.defensive_pat_succeeds CASCADE;
DROP VIEW public.game_player CASCADE;
DROP VIEW public.game_team CASCADE;

CREATE VIEW public.game_score AS(
SELECT
	a.gsis_id,
	a.team,
	CASE
		WHEN a.team = b.home_team THEN home_score
		WHEN a.team = b.away_team THEN away_score
		ELSE NULL END AS team_score,
	CASE
		WHEN a.team = b.away_team THEN home_score
		WHEN a.team = b.home_team THEN away_score
		ELSE NULL END AS oppo_score
FROM public.play_player a
LEFT JOIN public.game b ON (a.team = b.home_team OR a.team = b.away_team) AND a.gsis_id = b.gsis_id
GROUP BY a.gsis_id, a.team, team_score, oppo_score);

CREATE VIEW public.offensive_yds AS(
SELECT
	gsis_id,
	team,
	SUM(passing_yds + rushing_yds + receiving_yds + fumbles_rec_yds) AS offensive_yds
FROM public.play_player
GROUP BY gsis_id, team);

CREATE VIEW public.defensive_pat_succeeds AS(
SELECT
	a.gsis_id,
	CASE WHEN a.pos_team = b.home_team THEN b.away_team
		WHEN a.pos_team = b.away_team THEN b.home_team
		ELSE Null END AS team
FROM play a
LEFT JOIN game b ON a.gsis_id = b.gsis_id
WHERE a.description ~ 'DEFENSIVE TWO-POINT ATTEMPT' AND a.description ~ 'ATTEMPT SUCCEEDS');

CREATE VIEW public.game_player AS(
SELECT
	gsis_id,
	player_id,
	team,
	SUM(defense_ast) AS defense_ast,
	SUM(defense_ffum) AS defense_ffum,
	SUM(defense_fgblk) AS defense_fgblk,
	SUM(defense_frec) AS defense_frec,
	SUM(defense_frec_tds) AS defense_frec_tds,
	SUM(defense_frec_yds) AS defense_frec_yds,
	SUM(defense_int) AS defense_int,
	SUM(defense_int_tds) AS defense_int_tds,
	SUM(defense_int_yds) AS defense_int_yds,
	SUM(defense_misc_tds) AS defense_misc_tds,
	SUM(defense_misc_yds) AS defense_misc_yds,
	SUM(defense_pass_def) AS defense_pass_def,
	SUM(defense_puntblk) AS defense_puntblk,
	SUM(defense_qbhit) AS defense_qbhit,
	SUM(defense_safe) AS defense_safe,
	SUM(defense_sk) AS defense_sk,
	SUM(defense_sk_yds) AS defense_sk_yds,
	SUM(defense_tkl) AS defense_tkl,
	SUM(defense_tkl_loss) AS defense_tkl_loss,
	SUM(defense_tkl_loss_yds) AS defense_tkl_loss_yds,
	SUM(defense_tkl_primary) AS defense_tkl_primary,
	SUM(defense_xpblk) AS defense_xpblk,
	SUM(fumbles_forced) AS fumbles_forced,
	SUM(fumbles_lost) AS fumbles_lost,
	SUM(fumbles_notforced) AS fumbles_notforced,
	SUM(fumbles_oob) AS fumbles_oob,
	SUM(fumbles_rec) AS fumbles_rec,
	SUM(fumbles_rec_tds) AS fumbles_rec_tds,
	SUM(fumbles_rec_yds) AS fumbles_rec_yds,
	SUM(fumbles_tot) AS fumbles_tot,
	SUM(kicking_all_yds) AS kicking_all_yds,
	SUM(kicking_downed) AS kicking_downed,
	SUM(CASE
			WHEN kicking_fgm_yds < 40 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm0_espn,
	SUM(CASE
			WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm40_espn,
	SUM(CASE
			WHEN kicking_fgm_yds >= 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm50_espn,
	SUM(CASE
			WHEN kicking_fgm_yds < 20 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm0_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 20 AND kicking_fgm_yds < 30 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm20_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 30 AND kicking_fgm_yds < 40 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm30_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm40_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm50_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds < 20 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed0_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds >= 20 AND kicking_fgmissed_yds < 30 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed20_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds >= 30 AND kicking_fgmissed_yds < 40 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed30_yahoo,
	SUM(kicking_fga) AS kicking_fga,
	SUM(kicking_fgb) AS kicking_fgb,
	SUM(kicking_fgm) AS kicking_fgm,
	SUM(kicking_fgm_yds) AS kicking_fgm_yds,
	SUM(kicking_fgmissed) AS kicking_fgmissed,
	SUM(kicking_fgmissed_yds) AS kicking_fgmissed_yds,
	SUM(kicking_i20) AS kicking_i20,
	SUM(kicking_rec) AS kicking_rec,
	SUM(kicking_rec_tds) AS kicking_rec_tds,
	SUM(kicking_tot) AS kicking_tot,
	SUM(kicking_touchback) AS kicking_touchback,
	SUM(kicking_xpa) AS kicking_xpa,
	SUM(kicking_xpb) AS kicking_xpb,
	SUM(kicking_xpmade) AS kicking_xpmade,
	SUM(kicking_xpmissed) AS kicking_xpmissed,
	SUM(kicking_yds) AS kicking_yds,
	SUM(kickret_fair) AS kickret_fair,
	SUM(kickret_oob) AS kickret_oob,
	SUM(kickret_ret) AS kickret_ret,
	SUM(kickret_tds) AS kickret_tds,
	SUM(kickret_touchback) AS kickret_touchback,
	SUM(kickret_yds) AS kickret_yds,
	SUM(passing_att) AS passing_att,
	SUM(passing_cmp) AS passing_cmp,
	SUM(passing_cmp_air_yds) AS passing_cmp_air_yds,
	SUM(passing_incmp) AS passing_incmp,
	SUM(passing_incmp_air_yds) AS passing_incmp_air_yds,
	SUM(passing_int) AS passing_int,
	SUM(passing_sk) AS passing_sk,
	SUM(passing_sk_yds) AS passing_sk_yds,
	SUM(passing_tds) AS passing_tds,
	SUM(passing_twopta) AS passing_twopta,
	SUM(passing_twoptm) AS passing_twoptm,
	SUM(passing_twoptmissed) AS passing_twoptmissed,
	SUM(passing_yds) AS passing_yds,
	SUM(punting_blk) AS punting_blk,
	SUM(punting_i20) AS punting_i20,
	SUM(punting_tot) AS punting_tot,
	SUM(punting_touchback) AS punting_touchback,
	SUM(punting_yds) AS punting_yds,
	SUM(puntret_downed) AS puntret_downed,
	SUM(puntret_fair) AS puntret_fair,
	SUM(puntret_oob) AS puntret_oob,
	SUM(puntret_tds) AS puntret_tds,
	SUM(puntret_tot) AS puntret_tot,
	SUM(puntret_touchback) AS puntret_touchback,
	SUM(puntret_yds) AS puntret_yds,
	SUM(receiving_rec) AS receiving_rec,
	SUM(receiving_tar) AS receiving_tar,
	SUM(receiving_tds) AS receiving_tds,
	SUM(receiving_twopta) AS receiving_twopta,
	SUM(receiving_twoptm) AS receiving_twoptm,
	SUM(receiving_twoptmissed) AS receiving_twoptmissed,
	SUM(receiving_yac_yds) AS receiving_yac_yds,
	SUM(receiving_yds) AS receiving_yds,
	SUM(rushing_att) AS rushing_att,
	SUM(rushing_loss) AS rushing_loss,
	SUM(rushing_loss_yds) AS rushing_loss_yds,
	SUM(rushing_tds) AS rushing_tds,
	SUM(rushing_twopta) AS rushing_twopta,
	SUM(rushing_twoptm) AS rushing_twoptm,
	SUM(rushing_twoptmissed) AS rushing_twoptmissed,
	SUM(rushing_yds) AS rushing_yds
FROM public.play_player
GROUP BY gsis_id, player_id, team);

CREATE VIEW public.game_team AS(
SELECT
	gsis_id,
	team,
	SUM(defense_ast) AS defense_ast,
	SUM(defense_ffum) AS defense_ffum,
	SUM(defense_fgblk) AS defense_fgblk,
	SUM(defense_frec) AS defense_frec,
	SUM(defense_frec_tds) AS defense_frec_tds,
	SUM(defense_frec_yds) AS defense_frec_yds,
	SUM(defense_int) AS defense_int,
	SUM(defense_int_tds) AS defense_int_tds,
	SUM(defense_int_yds) AS defense_int_yds,
	SUM(defense_misc_tds) AS defense_misc_tds,
	SUM(defense_misc_yds) AS defense_misc_yds,
	SUM(defense_pass_def) AS defense_pass_def,
	SUM(defense_puntblk) AS defense_puntblk,
	SUM(defense_qbhit) AS defense_qbhit,
	SUM(defense_safe) AS defense_safe,
	SUM(defense_sk) AS defense_sk,
	SUM(defense_sk_yds) AS defense_sk_yds,
	SUM(defense_tkl) AS defense_tkl,
	SUM(defense_tkl_loss) AS defense_tkl_loss,
	SUM(defense_tkl_loss_yds) AS defense_tkl_loss_yds,
	SUM(defense_tkl_primary) AS defense_tkl_primary,
	SUM(defense_xpblk) AS defense_xpblk,
	SUM(fumbles_forced) AS fumbles_forced,
	SUM(fumbles_lost) AS fumbles_lost,
	SUM(fumbles_notforced) AS fumbles_notforced,
	SUM(fumbles_oob) AS fumbles_oob,
	SUM(fumbles_rec) AS fumbles_rec,
	SUM(fumbles_rec_tds) AS fumbles_rec_tds,
	SUM(fumbles_rec_yds) AS fumbles_rec_yds,
	SUM(fumbles_tot) AS fumbles_tot,
	SUM(kicking_all_yds) AS kicking_all_yds,
	SUM(kicking_downed) AS kicking_downed,
	SUM(CASE
			WHEN kicking_fgm_yds < 40 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm0_espn,
	SUM(CASE
			WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm40_espn,
	SUM(CASE
			WHEN kicking_fgm_yds >= 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm50_espn,
	SUM(CASE
			WHEN kicking_fgm_yds < 20 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm0_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 20 AND kicking_fgm_yds < 30 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm20_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 30 AND kicking_fgm_yds < 40 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm30_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 40 AND kicking_fgm_yds < 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm40_yahoo,
	SUM(CASE
			WHEN kicking_fgm_yds >= 50 THEN kicking_fgm
			ELSE 0 END) AS kicking_fgm50_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds < 20 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed0_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds >= 20 AND kicking_fgmissed_yds < 30 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed20_yahoo,
	SUM(CASE
	   		WHEN kicking_fgmissed_yds >= 30 AND kicking_fgmissed_yds < 40 THEN kicking_fgmissed
	   		ELSE 0 END) AS kicking_fgmissed30_yahoo,
	SUM(kicking_fga) AS kicking_fga,
	SUM(kicking_fgb) AS kicking_fgb,
	SUM(kicking_fgm) AS kicking_fgm,
	SUM(kicking_fgm_yds) AS kicking_fgm_yds,
	SUM(kicking_fgmissed) AS kicking_fgmissed,
	SUM(kicking_fgmissed_yds) AS kicking_fgmissed_yds,
	SUM(kicking_i20) AS kicking_i20,
	SUM(kicking_rec) AS kicking_rec,
	SUM(kicking_rec_tds) AS kicking_rec_tds,
	SUM(kicking_tot) AS kicking_tot,
	SUM(kicking_touchback) AS kicking_touchback,
	SUM(kicking_xpa) AS kicking_xpa,
	SUM(kicking_xpb) AS kicking_xpb,
	SUM(kicking_xpmade) AS kicking_xpmade,
	SUM(kicking_xpmissed) AS kicking_xpmissed,
	SUM(kicking_yds) AS kicking_yds,
	SUM(kickret_fair) AS kickret_fair,
	SUM(kickret_oob) AS kickret_oob,
	SUM(kickret_ret) AS kickret_ret,
	SUM(kickret_tds) AS kickret_tds,
	SUM(kickret_touchback) AS kickret_touchback,
	SUM(kickret_yds) AS kickret_yds,
	SUM(passing_att) AS passing_att,
	SUM(passing_cmp) AS passing_cmp,
	SUM(passing_cmp_air_yds) AS passing_cmp_air_yds,
	SUM(passing_incmp) AS passing_incmp,
	SUM(passing_incmp_air_yds) AS passing_incmp_air_yds,
	SUM(passing_int) AS passing_int,
	SUM(passing_sk) AS passing_sk,
	SUM(passing_sk_yds) AS passing_sk_yds,
	SUM(passing_tds) AS passing_tds,
	SUM(passing_twopta) AS passing_twopta,
	SUM(passing_twoptm) AS passing_twoptm,
	SUM(passing_twoptmissed) AS passing_twoptmissed,
	SUM(passing_yds) AS passing_yds,
	SUM(punting_blk) AS punting_blk,
	SUM(punting_i20) AS punting_i20,
	SUM(punting_tot) AS punting_tot,
	SUM(punting_touchback) AS punting_touchback,
	SUM(punting_yds) AS punting_yds,
	SUM(puntret_downed) AS puntret_downed,
	SUM(puntret_fair) AS puntret_fair,
	SUM(puntret_oob) AS puntret_oob,
	SUM(puntret_tds) AS puntret_tds,
	SUM(puntret_tot) AS puntret_tot,
	SUM(puntret_touchback) AS puntret_touchback,
	SUM(puntret_yds) AS puntret_yds,
	SUM(receiving_rec) AS receiving_rec,
	SUM(receiving_tar) AS receiving_tar,
	SUM(receiving_tds) AS receiving_tds,
	SUM(receiving_twopta) AS receiving_twopta,
	SUM(receiving_twoptm) AS receiving_twoptm,
	SUM(receiving_twoptmissed) AS receiving_twoptmissed,
	SUM(receiving_yac_yds) AS receiving_yac_yds,
	SUM(receiving_yds) AS receiving_yds,
	SUM(rushing_att) AS rushing_att,
	SUM(rushing_loss) AS rushing_loss,
	SUM(rushing_loss_yds) AS rushing_loss_yds,
	SUM(rushing_tds) AS rushing_tds,
	SUM(rushing_twopta) AS rushing_twopta,
	SUM(rushing_twoptm) AS rushing_twoptm,
	SUM(rushing_twoptmissed) AS rushing_twoptmissed,
	SUM(rushing_yds) AS rushing_yds
FROM public.play_player
GROUP BY gsis_id, team);

CREATE VIEW public.espn_player_points_subtotal AS(
SELECT
	player_id,
	gsis_id,
	team,
	passing_yds / 25 AS passing_yds,
	passing_tds * 4 AS passing_tds,
	passing_int * (-2) AS passing_int,
	passing_twoptm * 2 AS passing_twoptm,
	rushing_yds / 10 AS rushing_yds,
	rushing_tds * 6 AS rushing_tds,
	rushing_twoptm * 2 AS rushing_twoptm,
	receiving_yds / 10 AS receiving_yds,
	receiving_tds * 6 AS receiving_tds,
	receiving_twoptm * 2 AS receiving_twoptm,
	kickret_tds * 6 AS kickret_tds,
	puntret_tds * 6 AS puntret_tds,
	fumbles_rec_tds * 6 AS fumbles_rec_tds,
	fumbles_lost * (-2) AS fumbles_lost,
	defense_int_tds * 6 AS defense_int_tds,
	defense_frec_tds * 6 AS defense_frec_tds,
	defense_misc_tds * 6 AS defense_misc_tds,
	kicking_xpmade AS kicking_xpmade,
	kicking_fgmissed * (-1) AS kicking_fgmissed,
	(kicking_fgm0_espn * 3) + (kicking_fgm40_espn * 4) + (kicking_fgm50_espn * 5) AS kicking_fgm,
	(defense_puntblk + kicking_xpb + defense_fgblk) * 2 AS defense_blk,
	defense_int * 2 AS defense_int,
	defense_frec * 2 AS defense_frec,
	defense_safe * 2 AS defense_safe
FROM public.game_player);

CREATE VIEW public.yahoo_player_points_subtotal AS(
SELECT
	player_id,
	gsis_id,
	team,
	passing_yds / 25 AS passing_yds,
	passing_tds * 4 AS passing_tds,
	passing_int * (-1) AS passing_int,
	rushing_yds / 10 AS rushing_yds,
	rushing_tds * 6 AS rushing_tds,
	receiving_rec / 2 AS receiving_rec,
	receiving_yds / 10 AS receiving_yds,
	receiving_tds * 6 AS receiving_tds,
	kickret_tds * 6 AS kickret_tds,
	puntret_tds * 6 AS puntret_tds,
	passing_twoptm * 2 AS passing_twoptm,
	rushing_twoptm * 2 AS rushing_twoptm,
	receiving_twoptm * 2 AS receiving_twoptm,
	fumbles_lost * (-2) AS fumbles_lost,
	fumbles_rec_tds * 6 AS fumbles_rec_tds,
	(kicking_fgm0_yahoo * 3) + (kicking_fgm20_yahoo * 3) + (kicking_fgm30_yahoo * 3) + (kicking_fgm40_yahoo * 4) + (kicking_fgm50_yahoo * 5) AS kicking_fgm,
	(kicking_fgmissed0_yahoo * (-1)) + (kicking_fgmissed20_yahoo * (-1)) + (kicking_fgmissed30_yahoo * (-1)) AS kicking_fgmissed,
	kicking_xpmade AS kicking_xpmade
FROM public.game_player);

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
	 + kicking_fgm + kicking_fgmissed + kicking_xpmade) AS fantasy_points
FROM public.yahoo_player_points_subtotal);

CREATE VIEW public.espn_defense_points_subtotal AS(
SELECT
	gsis_id,
	team,
	defense_sk AS defense_sk,
	defense_int_tds * 6 AS defense_int_tds,
	defense_frec_tds * 6 AS defense_frec_tds,
	kickret_tds * 6 AS kickret_tds,
	puntret_tds * 6 AS puntret_tds,
	defense_misc_tds * 6 AS defense_misc_tds,
	(defense_puntblk + kicking_xpb + defense_fgblk) * 2 AS defense_blk,
	defense_int * 2 AS defense_int,
	defense_frec * 2 AS defense_frec,
	defense_safe * 2 AS defense_safe
FROM public.game_team);

CREATE VIEW public.yahoo_defense_points_subtotal AS(
SELECT
	gsis_id,
	team,
	defense_sk AS defense_sk,
	defense_int * 2 AS defense_int,
	defense_frec * 2 AS defense_frec,
	defense_int_tds * 6 AS defense_int_tds,
	defense_frec_tds * 6 AS defense_frec_tds,
	defense_misc_tds * 6 AS defense_misc_tds,
	defense_safe * 2 AS defense_safe,
	(defense_puntblk + kicking_xpb + defense_fgblk) * 2 AS defense_blk,
	kickret_tds * 6 AS kickret_tds,
	puntret_tds * 6 AS puntret_tds
FROM public.game_team);

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
LEFT JOIN public.offensive_yds b ON a.gsis_id = b.gsis_id AND a.team = b.team);

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
LEFT JOIN public.defensive_pat_succeeds b ON a.gsis_id = b.gsis_id AND a.team = b.team);

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
LEFT JOIN public.espn_defense_points_subset b ON a.gsis_id = b.gsis_id AND a.team = b.team);

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
LEFT JOIN public.yahoo_defense_points_subset b ON a.gsis_id = b.gsis_id AND a.team = b.team);

CREATE VIEW public.fantasy_football_points_total AS(
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points,
	'espn' AS fantasy_platform
FROM public.espn_player_points_total
UNION
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points,
	'espn' AS fantasy_platform
FROM public.espn_defense_points_total
UNION
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points,
	'yahoo' AS fantasy_platform
FROM public.yahoo_player_points_total
UNION
SELECT
	player_id,
	gsis_id,
	team,
	fantasy_points,
	'yahoo' AS fantasy_platform
FROM public.yahoo_defense_points_total);

CREATE VIEW public.fantasy_player AS(
SELECT
	a.gsis_id,
	a.player_id,
	a.team,
	a.defense_ast,
	a.defense_ffum,
	a.defense_fgblk,
	a.defense_frec,
	a.defense_frec_tds,
	a.defense_frec_yds,
	a.defense_int,
	a.defense_int_tds,
	a.defense_int_yds,
	a.defense_misc_tds,
	a.defense_misc_yds,
	a.defense_pass_def,
	a.defense_puntblk,
	a.defense_qbhit,
	a.defense_safe,
	a.defense_sk,
	a.defense_sk_yds,
	a.defense_tkl,
	a.defense_tkl_loss,
	a.defense_tkl_loss_yds,
	a.defense_tkl_primary,
	a.defense_xpblk,
	a.fumbles_forced,
	a.fumbles_lost,
	a.fumbles_notforced,
	a.fumbles_oob,
	a.fumbles_rec,
	a.fumbles_rec_tds,
	a.fumbles_rec_yds,
	a.fumbles_tot,
	a.kicking_all_yds,
	a.kicking_downed,
	a.kicking_fgm0_espn,
	a.kicking_fgm40_espn,
	a.kicking_fgm50_espn,
	a.kicking_fgm0_yahoo,
	a.kicking_fgm20_yahoo,
	a.kicking_fgm30_yahoo,
	a.kicking_fgm40_yahoo,
	a.kicking_fgm50_yahoo,
	a.kicking_fgmissed0_yahoo,
	a.kicking_fgmissed20_yahoo,
	a.kicking_fgmissed30_yahoo,
	a.kicking_fga,
	a.kicking_fgb,
	a.kicking_fgm,
	a.kicking_fgm_yds,
	a.kicking_fgmissed,
	a.kicking_fgmissed_yds,
	a.kicking_i20,
	a.kicking_rec,
	a.kicking_rec_tds,
	a.kicking_tot,
	a.kicking_touchback,
	a.kicking_xpa,
	a.kicking_xpb,
	a.kicking_xpmade,
	a.kicking_xpmissed,
	a.kicking_yds,
	a.kickret_fair,
	a.kickret_oob,
	a.kickret_ret,
	a.kickret_tds,
	a.kickret_touchback,
	a.kickret_yds,
	a.passing_att,
	a.passing_cmp,
	a.passing_cmp_air_yds,
	a.passing_incmp,
	a.passing_incmp_air_yds,
	a.passing_int,
	a.passing_sk,
	a.passing_sk_yds,
	a.passing_tds,
	a.passing_twopta,
	a.passing_twoptm,
	a.passing_twoptmissed,
	a.passing_yds,
	a.punting_blk,
	a.punting_i20,
	a.punting_tot,
	a.punting_touchback,
	a.punting_yds,
	a.puntret_downed,
	a.puntret_fair,
	a.puntret_oob,
	a.puntret_tds,
	a.puntret_tot,
	a.puntret_touchback,
	a.puntret_yds,
	a.receiving_rec,
	a.receiving_tar,
	a.receiving_tds,
	a.receiving_twopta,
	a.receiving_twoptm,
	a.receiving_twoptmissed,
	a.receiving_yac_yds,
	a.receiving_yds,
	a.rushing_att,
	a.rushing_loss,
	a.rushing_loss_yds,
	a.rushing_tds,
	a.rushing_twopta,
	a.rushing_twoptm,
	a.rushing_twoptmissed,
	a.rushing_yds,
	b.fantasy_points,
	b.fantasy_platform
FROM public.game_player a
LEFT JOIN public.fantasy_football_points_total b ON a.player_id = b.player_id AND a.gsis_id = b.gsis_id);

CREATE VIEW public.fantasy_defense AS(
SELECT
	a.gsis_id,
	b.player_id,
	a.team,
	a.defense_ast,
	a.defense_ffum,
	a.defense_fgblk,
	a.defense_frec,
	a.defense_frec_tds,
	a.defense_frec_yds,
	a.defense_int,
	a.defense_int_tds,
	a.defense_int_yds,
	a.defense_misc_tds,
	a.defense_misc_yds,
	a.defense_pass_def,
	a.defense_puntblk,
	a.defense_qbhit,
	a.defense_safe,
	a.defense_sk,
	a.defense_sk_yds,
	a.defense_tkl,
	a.defense_tkl_loss,
	a.defense_tkl_loss_yds,
	a.defense_tkl_primary,
	a.defense_xpblk,
	a.fumbles_forced,
	a.fumbles_lost,
	a.fumbles_notforced,
	a.fumbles_oob,
	a.fumbles_rec,
	a.fumbles_rec_tds,
	a.fumbles_rec_yds,
	a.fumbles_tot,
	a.kicking_all_yds,
	a.kicking_downed,
	a.kicking_fgm0_espn,
	a.kicking_fgm40_espn,
	a.kicking_fgm50_espn,
	a.kicking_fgm0_yahoo,
	a.kicking_fgm20_yahoo,
	a.kicking_fgm30_yahoo,
	a.kicking_fgm40_yahoo,
	a.kicking_fgm50_yahoo,
	a.kicking_fgmissed0_yahoo,
	a.kicking_fgmissed20_yahoo,
	a.kicking_fgmissed30_yahoo,
	a.kicking_fga,
	a.kicking_fgb,
	a.kicking_fgm,
	a.kicking_fgm_yds,
	a.kicking_fgmissed,
	a.kicking_fgmissed_yds,
	a.kicking_i20,
	a.kicking_rec,
	a.kicking_rec_tds,
	a.kicking_tot,
	a.kicking_touchback,
	a.kicking_xpa,
	a.kicking_xpb,
	a.kicking_xpmade,
	a.kicking_xpmissed,
	a.kicking_yds,
	a.kickret_fair,
	a.kickret_oob,
	a.kickret_ret,
	a.kickret_tds,
	a.kickret_touchback,
	a.kickret_yds,
	a.passing_att,
	a.passing_cmp,
	a.passing_cmp_air_yds,
	a.passing_incmp,
	a.passing_incmp_air_yds,
	a.passing_int,
	a.passing_sk,
	a.passing_sk_yds,
	a.passing_tds,
	a.passing_twopta,
	a.passing_twoptm,
	a.passing_twoptmissed,
	a.passing_yds,
	a.punting_blk,
	a.punting_i20,
	a.punting_tot,
	a.punting_touchback,
	a.punting_yds,
	a.puntret_downed,
	a.puntret_fair,
	a.puntret_oob,
	a.puntret_tds,
	a.puntret_tot,
	a.puntret_touchback,
	a.puntret_yds,
	a.receiving_rec,
	a.receiving_tar,
	a.receiving_tds,
	a.receiving_twopta,
	a.receiving_twoptm,
	a.receiving_twoptmissed,
	a.receiving_yac_yds,
	a.receiving_yds,
	a.rushing_att,
	a.rushing_loss,
	a.rushing_loss_yds,
	a.rushing_tds,
	a.rushing_twopta,
	a.rushing_twoptm,
	a.rushing_twoptmissed,
	a.rushing_yds,
	b.fantasy_points,
	b.fantasy_platform
FROM public.game_team a
LEFT JOIN public.fantasy_football_points_total b ON a.team  || ' D/ST' = b.player_id AND a.gsis_id = b.gsis_id);

CREATE VIEW public.fantasy_analytics AS(
SELECT
	*
FROM public.fantasy_player
UNION
SELECT
	*
FROM public.fantasy_defense);
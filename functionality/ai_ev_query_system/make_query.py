from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any, List, Callable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

market_key_to_player_prop = {
    'batter_hits': 'batting_hits',
    'batter_runs_scored': 'batting_runs_scored',
    'batter_rbis': 'batting_runs_batted_in',
}

over_under_dict = {
    "Over": ">",
    "Under": "<"
}

class Query:
    def __init__(self, name: str, query_func: Callable, query_params: Dict[str, Any], exec_params: Dict[str, Any]):
        self.name = name
        self.query_func = query_func
        self.query_params = query_params
        self.exec_params = exec_params

class PlayerPerformanceAnalyzer:
    def __init__(self, engine):
        self.engine = engine

    def analyze_performance(self, player_name: str, queries: List[Query], target_threshold: float, over_under: str) -> Dict[str, Any]:
        for query in queries:
            result = self._execute_query(query, player_name)
            if result.get('error'):
                logger.error(f"Error in {query.name}: {result['error']}")
                continue

            formatted_result = self._format_result(result, query, target_threshold, over_under)
            logger.info(self._format_output(player_name, query.exec_params['stat_column'], formatted_result, over_under))

            if self._check_threshold(formatted_result['proportion_over_threshold'], target_threshold, over_under):
                logger.info(f"Threshold met in: {query.name}")
                return formatted_result

        return {"error": "No query met the target threshold"}

    def _execute_query(self, query: Query, player_name: str) -> Dict[str, Any]:
        try:
            with Session(self.engine) as session:
                query_text = query.query_func(**query.query_params)
                result = session.execute(text(query_text), {"player_name": player_name, **query.exec_params}).fetchone()
                if result:
                    return dict(zip(["total_games", "games_over_threshold", "proportion_over_threshold", "average_stat"], result))
                else:
                    return {"error": f"No data found for player {player_name} in {query.name}"}
        except SQLAlchemyError as e:
            logger.error(f"Database error in {query.name}: {str(e)}")
            return {"error": f"Database error occurred in {query.name}"}

    def _format_result(self, result: Dict[str, Any], query: Query, target_threshold: float, over_under: str) -> Dict[str, Any]:
        return {
            **result,
            "analysis_stage": query.name,
            "opponent": query.exec_params.get('opponent'),
            "threshold_met": self._check_threshold(result['proportion_over_threshold'], target_threshold, over_under),
            "target_threshold": target_threshold,
            "exec_params": query.exec_params
        }

    def _check_threshold(self, proportion: float, target: float, over_under: str) -> bool:

        return proportion >= target
        

    def _format_output(self, player_name: str, stat_name: str, result: Dict[str, Any], over_under: str) -> str:
        if "error" in result:
            return f"Error: {result['error']}"

        def format_stat(value, format_spec=":.2f"):
            if value is None:
                return "N/A"
            try:
                return f"{float(value):{format_spec}}"
            except (ValueError, TypeError):
                return str(value)

        output = f"""
        Player Performance Analysis for {player_name}:
        Analysis Stage: {result['analysis_stage']}
        Stat Analyzed: {stat_name} {over_under} {result['exec_params']['stat_threshold']}
        Target Threshold: {format_stat(result['target_threshold'], '.2%')}
        Threshold Met: {'Yes' if result['threshold_met'] else 'No'}
        Total Games Considered: {result.get('total_games', 'N/A')}
        Games {over_under} Threshold: {result.get('games_over_threshold', 'N/A')}
        Proportion {over_under} Threshold: {format_stat(result.get('proportion_over_threshold'), '.2%')}
        Average {stat_name}: {format_stat(result.get('average_stat'))}
        """
        return output

def last_n_games_query(stat_column: str, limit: int, over_under: str) -> str:
    return f"""
    WITH player_info AS (
        SELECT player_id
        FROM sport_player_new
        WHERE full_name = :player_name
    ),
    player_games AS (
        SELECT mpa.game_id, mpa.{stat_column}
        FROM mlb_player_appearance_new mpa
        JOIN sport_game_new sg ON mpa.game_id = sg.game_id
        WHERE mpa.player_id = (SELECT player_id FROM player_info)
        ORDER BY sg.game_id DESC
        LIMIT {limit}
    ),
    stat_counts AS (
        SELECT 
            game_id,
            {stat_column},
            CASE WHEN {stat_column} {over_under_dict[over_under]} :stat_threshold THEN 1 ELSE 0 END AS over_threshold
        FROM player_games
    )
    SELECT 
        COUNT(*) AS total_games,
        SUM(over_threshold) AS games_over_threshold,
        CASE 
            WHEN COUNT(*) > 0 THEN CAST(SUM(over_threshold) AS FLOAT) / COUNT(*) 
            ELSE 0 
        END AS proportion_over_threshold,
        AVG({stat_column}) AS average_stat
    FROM stat_counts;
    """


def last_n_games_at_night_query(stat_column: str, limit: int, over_under: str) -> str:
    return f"""
    WITH player_info AS (
        SELECT player_id
        FROM sport_player_new
        WHERE full_name = :player_name
    ),
    player_games AS (
        SELECT mpa.game_id, mpa.{stat_column}
        FROM mlb_player_appearance_new mpa
        JOIN sport_game_new sg ON mpa.game_id = sg.game_id
        WHERE mpa.player_id = (SELECT player_id FROM player_info)
        AND sg.game_day_or_night = 'Night'
        ORDER BY sg.game_id DESC
        LIMIT {limit}
    ),
    stat_counts AS (
        SELECT 
            game_id,
            {stat_column},
            CASE WHEN {stat_column} {over_under_dict[over_under]} :stat_threshold THEN 1 ELSE 0 END AS over_threshold
        FROM player_games
    )
    SELECT 
        COUNT(*) AS total_games,
        SUM(over_threshold) AS games_over_threshold,
        CASE 
            WHEN COUNT(*) > 0 THEN CAST(SUM(over_threshold) AS FLOAT) / COUNT(*) 
            ELSE 0 
        END AS proportion_over_threshold,
        AVG({stat_column}) AS average_stat
    FROM stat_counts;
    """


def last_n_games_vs_opponent_query(stat_column: str, limit: int, over_under: str) -> str:
    return f"""
    WITH player_info AS (
        SELECT player_id
        FROM sport_player_new
        WHERE full_name = :player_name
    ),
    player_team AS (
        SELECT spth.team_id, st.team_name
        FROM sport_player_team_history_new spth
        JOIN sport_team_new st ON spth.team_id = st.team_id
        WHERE spth.player_id = (SELECT player_id FROM player_info)
        AND spth.is_current_team = 1
    ),
    game_teams AS (
        SELECT 
            st_home.team_id AS home_team_id,
            st_home.team_name AS home_team_name,
            st_away.team_id AS away_team_id,
            st_away.team_name AS away_team_name
        FROM sport_team_new st_home
        CROSS JOIN sport_team_new st_away
        WHERE st_home.team_name = :home_team AND st_away.team_name = :away_team
    ),
    opponent_team AS (
        SELECT 
            CASE 
                WHEN gt.home_team_name = (SELECT team_name FROM player_team) THEN gt.away_team_id
                ELSE gt.home_team_id
            END AS opponent_team_id
        FROM game_teams gt
    ),
    player_games AS (
        SELECT 
            mpa.game_id, 
            mpa.{stat_column}, 
            sg.home_team_id, 
            sg.away_team_id
        FROM mlb_player_appearance_new mpa
        JOIN sport_game_new sg ON mpa.game_id = sg.game_id
        WHERE mpa.player_id = (SELECT player_id FROM player_info)
        ORDER BY sg.game_id DESC
    ),
    relevant_games AS (
        SELECT 
            pg.game_id, 
            pg.{stat_column}
        FROM player_games pg
        CROSS JOIN opponent_team ot
        WHERE 
            (pg.home_team_id = (SELECT team_id FROM player_team) AND pg.away_team_id = ot.opponent_team_id)
            OR 
            (pg.away_team_id = (SELECT team_id FROM player_team) AND pg.home_team_id = ot.opponent_team_id)
        LIMIT {limit}
    ),
    stat_counts AS (
        SELECT 
            game_id,
            {stat_column},
            CASE WHEN {stat_column} {over_under_dict[over_under]} :stat_threshold THEN 1 ELSE 0 END AS over_threshold
        FROM relevant_games
    )
    SELECT 
        COUNT(*) AS total_games,
        SUM(over_threshold) AS games_over_threshold,
        CASE 
            WHEN COUNT(*) > 0 THEN CAST(SUM(over_threshold) AS FLOAT) / COUNT(*) 
            ELSE 0 
        END AS proportion_over_threshold,
        AVG({stat_column}) AS average_stat
    FROM stat_counts;
    """


def career_stats_query(stat_column: str, over_under: str) -> str:
    return f"""
    WITH player_info AS (
        SELECT player_id
        FROM sport_player_new
        WHERE full_name = :player_name
    ),
    player_games AS (
        SELECT mpa.game_id, mpa.{stat_column}
        FROM mlb_player_appearance_new mpa
        WHERE mpa.player_id = (SELECT player_id FROM player_info)
    ),
    stat_counts AS (
        SELECT 
            game_id,
            {stat_column},
            CASE WHEN {stat_column} {over_under_dict[over_under]} :stat_threshold THEN 1 ELSE 0 END AS over_threshold
        FROM player_games
    )
    SELECT 
        COUNT(*) AS total_games,
        SUM(over_threshold) AS games_over_threshold,
        CASE 
            WHEN COUNT(*) > 0 THEN CAST(SUM(over_threshold) AS FLOAT) / COUNT(*) 
            ELSE 0 
        END AS proportion_over_threshold,
        AVG({stat_column}) AS average_stat
    FROM stat_counts;
    """

from .db_manager import DBManager

def make_query(player_name: str, stat_column: str, stat_threshold: str, home_team: str, away_team: str, target_threshold: float, over_under: str, games_in_past: int = 20):
    db_manager = DBManager()
    analyzer = PlayerPerformanceAnalyzer(db_manager.get_engine())

    n_range_1 = [5, 6, 7, 8, 9, 10, 15, 20]
    n_range_2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, games_in_past]

    stat_column = market_key_to_player_prop[stat_column]

    queries = [
        Query(f"Last {n} games", last_n_games_query, 
            {"stat_column": stat_column, "limit": n, "over_under": over_under},
            {"stat_column": stat_column, "stat_threshold": stat_threshold})
        for n in n_range_1
    ] + [
        Query(f"Last {n} games vs opponent", last_n_games_vs_opponent_query, 
            {"stat_column": stat_column, "limit": n, "over_under": over_under},
            {"player_name": player_name, "stat_column": stat_column, "stat_threshold": stat_threshold,
            "home_team": home_team, "away_team": away_team})
        for n in n_range_2
    ] + [
    Query(f"Last {n} night games", last_n_games_at_night_query, {"stat_column": stat_column, "limit": n, "over_under": over_under}, {"stat_column": stat_column, "stat_threshold": stat_threshold})
    for n in [5, 6, 7, 8, 9, 10, 15, 20]
] + [
        Query("Career stats", career_stats_query, {"stat_column": stat_column, "over_under": over_under}, {"stat_column": stat_column, "stat_threshold": stat_threshold})
    ]
    
    result = analyzer.analyze_performance(player_name, queries, target_threshold, over_under)

    if "error" in result:
        logger.error(f"Final result: {result['error']}")
    else:
        logger.info(f"Analysis completed. Threshold met in: {result['analysis_stage']}")
    logger.info(result)
    return result

if __name__ == "__main__":
    make_query(
        player_name="Ernie Clement",
        stat_column="batting_hits",
        stat_threshold="0.5",
        over_under="Over",
        target_threshold=0.9,
        games_in_past=20,
        home_team = "Boston Red Sox",
        away_team = "Toronto Blue Jays",
    )
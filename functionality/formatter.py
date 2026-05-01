# TODO: MAKE THE OUTCOME_NAME, OUTCOME_DESCRIPTION AND OUTCOME_POINT
# TODO: MAKE THE MERGE COLUMN KEY AND merge_column_key_other
# TODO: Make the outcome, wagers, bet_type, sport_title_display	sport_league_display, market_display, wager_display, wager_display_other, snapshot_time columns

import re
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from unidecode import unidecode

try:
    from .db_manager import DBManager
except ImportError:
    from db_manager import DBManager


class Formatter:
    def __init__(self, df):
        self.df = df
        self.db_manager = DBManager()
        self._odds_api_game_id_map = self._build_odds_api_game_id_map()
        self._names_in_odds_map = {
            name for (h, a) in self._odds_api_game_id_map for name in (h, a)
        }
        self._name_conversion_map = self._build_name_conversion_map()
    
    def _build_name_conversion_map(self):
        """Query odds_pipeline.bet_info for MMA and build (home_team, away_team) -> odds_api_game_id mapping."""
        query = text("""
            SELECT DISTINCT 
            team_name, odds_api_team_name
            FROM bet_engine.odds_api_team_names_to_sports_reference
        """)
        with self.db_manager.get_engine().connect() as conn:
            odds_api_team_names_to_sports_reference_df = pd.read_sql(query, conn)

        # Dict keyed by team_name/HOF name (unidecoded) -> odds_api_team_name for name matching
        mapping = {}
        for _, row in odds_api_team_names_to_sports_reference_df.iterrows():
            key = unidecode(str(row['team_name']).strip()) if pd.notnull(row['team_name']) else ''
            val = unidecode(str(row['odds_api_team_name']).strip()) if pd.notnull(row['odds_api_team_name']) else ''
            mapping[key] = val
        mapping['Paulo Henrique Costa'] = 'Paulo Costa'
        mapping['Youssef Zalaal'] = 'Youssef Zalal'
        return mapping
    
    def _build_odds_api_game_id_map(self):
        """Query odds_pipeline.bet_info for MMA and build (home_team, away_team) -> (game_id, commence_time) mapping."""
        query = text("""
            SELECT DISTINCT home_team, away_team, game_id, commence_time
            FROM odds_pipeline.bet_info
            WHERE sport_league_display = 'MMA'
            AND game_id IS NOT NULL
        """)
        with self.db_manager.get_engine().connect() as conn:
            bet_info_df = pd.read_sql(query, conn)

        # Dict keyed by (home_team, away_team) -> (game_id, commence_time) (unidecoded for matching)
        mapping = {}
        for _, row in bet_info_df.iterrows():
            h = unidecode(str(row['home_team']).strip()) if pd.notnull(row['home_team']) else ''
            a = unidecode(str(row['away_team']).strip()) if pd.notnull(row['away_team']) else ''
            mapping[(h, a)] = (row['game_id'], row['commence_time'])
        return mapping

    def find_odds_api_game_id(self):
        self.df['home_team'] = self.df['home_team'].apply(
            lambda x: unidecode(str(x)) if pd.notnull(x) else x
        )
        self.df['away_team'] = self.df['away_team'].apply(
            lambda x: unidecode(str(x)) if pd.notnull(x) else x
        )

        def _lookup(row):
            home, away = row['home_team'], row['away_team']
            home, away = str(home).strip().replace('.', ''), str(away).strip().replace('.', '')
            # First try (home, away) and (away, home) in main mapping
            result = self._odds_api_game_id_map.get((home, away)) or self._odds_api_game_id_map.get((away, home))
            if result:
                return result

            # Only convert the name that's not found in the odds map
            if home in self._names_in_odds_map and away not in self._names_in_odds_map:
                new_away = self._name_conversion_map.get(away, away)
                result = self._odds_api_game_id_map.get((home, new_away)) or self._odds_api_game_id_map.get((new_away, home))
            elif away in self._names_in_odds_map and home not in self._names_in_odds_map:
                new_home = self._name_conversion_map.get(home, home)
                result = self._odds_api_game_id_map.get((new_home, away)) or self._odds_api_game_id_map.get((away, new_home))
            else:
                result = None

            if result:
                return result

            # Last fallback, cannot find
            return (None, None)

        lookup_results = self.df.apply(_lookup, axis=1)
        self.df['odds_api_game_id'] = [r[0] for r in lookup_results]
        self.df['game_id'] = self.df['odds_api_game_id']
        self.df['commence_time'] = [r[1] for r in lookup_results]
        return self.df

    def format_sport_title(self):
        self.df['sport_title'] = 'mma_mixed_martial_arts'
        self.df['sport_title_display'] = 'Mixed Martial Arts'
        self.df['sport_league_display'] = 'MMA'
        return self.df

    def format_market_key(self):
        """Transform existing market_key: h2h->h2h, totals->alternate_totals, Main Total->totals.
        When blank or invalid: derive from market_type/odds + market columns."""

        def _resolve_category(row):
            """Return the market category string, checking market_type then odds.

            Some HOF API rows have the category in 'odds' (not market_type) when
            market_key is incorrectly set to the fighter name.
            """
            for col in ('market_type', 'odds'):
                val = row.get(col)
                if pd.isnull(val):
                    continue
                s = str(val).strip()
                # Skip yes/no flags ('0'/'1'), empty, nan, and JSON blobs
                if s and s not in ('0', '1', 'nan') and not s.startswith('{'):
                    return s
            return ''

        def _derive_from_category(category, market):
            """Given a category name and market display string, return the market_key."""
            mt_lower = category.lower()
            m = market.lower()

            if mt_lower == 'alt totals':
                return 'alternate_totals'
            if mt_lower == 'distance (y/n)':
                return 'fight_to_go_the_distance'
            if mt_lower == 'fight lines':
                return 'h2h'
            if mt_lower == 'double chance':
                has_ko_tko = bool(re.search(r'ko[/\s]*tko|tko|ko', m))
                has_decision = 'decision' in m
                has_submission = 'submission' in m
                if has_ko_tko and has_decision and not has_submission:
                    return 'player_to_win_by_ko_or_tko_or_decision'
                if has_ko_tko and has_submission and not has_decision:
                    return 'player_to_win_by_ko_or_tko_or_submission'
                if has_submission and has_decision and not has_ko_tko:
                    return 'player_to_win_by_submission_or_decision'
            if mt_lower == 'method of victory':
                if re.search(r'fight\s+ends?\s+by\s+ko', m) or re.search(r'fight\s+ends?\s+.*ko.*tko.*dq', m):
                    return 'fight_to_end_by_ko_or_tko_or_dq'
                if re.search(r'fight\s+ends?\s+.*submission', m):
                    return 'fight_to_end_by_submission'
                if re.search(r'fight\s+goes?\s+the\s+distance', m):
                    return 'fight_to_go_the_distance'
                if re.search(r'wins?\s+by\s+(tko[/\s]*ko|ko[/\s]*tko)\b', m):
                    return 'player_to_win_by_ko_or_tko'
                if re.search(r'wins?\s+by\s+ko\b', m) and 'tko' not in m:
                    return 'player_to_win_by_ko'
                if re.search(r'wins?\s+by\s+submission\b', m):
                    return 'player_to_win_by_submission'
                if re.search(r'wins?\s+by\s+decision\b', m):
                    return 'player_to_win_by_decision'
            if mt_lower == 'round props':
                round_match = re.search(r'wins?\s+in\s+round\s+(\d)', m)
                if round_match:
                    return f'player_to_win_in_round_{round_match.group(1)}'
            if mt_lower == 'other props':
                if re.search(r'fight\s+doesn\'?t\s+end\s+by\s+ko', m) or re.search(r'fight\s+doesn\'?t\s+end.*ko.*tko.*dq', m):
                    return 'fight_to_end_by_ko_or_tko_or_dq'
                if re.search(r'fight\s+doesn\'?t\s+end.*submission', m):
                    return 'fight_to_end_by_submission'
                if re.search(r'doesn\'?t\s+win\s+by\s+(tko[/\s]*ko|ko[/\s]*tko)', m):
                    return 'player_to_win_by_ko_or_tko'
                if re.search(r'doesn\'?t\s+win\s+by\s+decision\b', m):
                    return 'player_to_win_by_decision'
                if re.search(r'doesn\'?t\s+win\s+by\s+submission\b', m):
                    return 'player_to_win_by_submission'
                if re.search(r'doesn\'?t\s+win\s+by\s+ko\b', m) and 'tko' not in m:
                    return 'player_to_win_by_ko'
                # "Fight doesn't start round N" / "Fight ends in round N" -> alternate_totals
                if re.search(r"fight\s+doesn'?t\s+start\s+round|fight\s+ends?\s+in\s+round", m):
                    return 'alternate_totals'
            if mt_lower == 'method + round':
                round_match = re.search(r'wins?\s+in\s+round\s+(\d)', m)
                if round_match:
                    r_n = round_match.group(1)
                    if re.search(r'ko|tko|dq', m):
                        return f'player_to_win_by_ko_or_tko_or_dq_round_{r_n}'
                    if 'submission' in m:
                        return f'player_to_win_by_submission_round_{r_n}'
            return None

        def _transform(row):
            orig = row['market_key']
            if pd.notnull(orig) and str(orig).strip():
                s = str(orig).strip()
                if s == 'h2h':
                    return 'h2h'
                if s == 'totals':
                    return 'alternate_totals'
                if s == 'Main Total':
                    return 'totals'
                # Valid snake_case market key — pass through unchanged
                if re.match(r'^[a-z][a-z0-9_]*$', s):
                    return s
                # Invalid value (e.g. fighter name leaked into column): fall through
                # to derive the correct key from category + market description below.

            market = str(row.get('market', '')).strip()
            category = _resolve_category(row)

            if category:
                result = _derive_from_category(category, market)
                if result:
                    return result

            # Last-resort: parse market display text for round patterns
            market_lower = market.lower()
            round_match = re.search(r'wins?\s+in\s+round\s+(\d)', market_lower)
            if round_match:
                r_n = round_match.group(1)
                if re.search(r'ko|tko|dq', market_lower):
                    return f'player_to_win_by_ko_or_tko_or_dq_round_{r_n}'
                if 'submission' in market_lower:
                    return f'player_to_win_by_submission_round_{r_n}'

            return None  # Will be dropped by format_cleanup

        if 'market_key' in self.df.columns:
            self.df['market_key'] = self.df.apply(_transform, axis=1)
        return self.df

    def format_outcome_description_name_point(self):
        """Set outcome_name, outcome_description, outcome_point based on market_key and market."""
        def _player_outcome_description(row):
            market = str(row.get('market', '')).strip()
            home = str(row.get('home_team', '')).strip()
            away = str(row.get('away_team', '')).strip()
            match = re.match(r'^(.+?)\s+(?:wins|doesn\'?t)', market, re.IGNORECASE)
            if not match:
                return None
            fighter_in_market = unidecode(match.group(1).strip()).lower()
            home_norm = unidecode(home).lower()
            away_norm = unidecode(away).lower()
            if fighter_in_market in home_norm or home_norm in fighter_in_market:
                return home
            if fighter_in_market in away_norm or away_norm in fighter_in_market:
                return away
            return None

        def _row_transform(row):
            market_key = str(row.get('market_key', '') or '')
            market = str(row.get('market', '')).strip()

            if market_key == 'h2h':
                outcome_name = unidecode(market)  # Fighter name
                outcome_description = None
                outcome_point = None
            elif 'totals' in market_key:
                # Over/Under X rounds
                over_under = re.match(r'^(over|under)\s+([\d.]+)', market, re.IGNORECASE)
                if over_under:
                    outcome_name = over_under.group(1).capitalize()
                    outcome_point = float(over_under.group(2))
                else:
                    outcome_name = None
                    outcome_point = None
                outcome_description = None
            elif 'fight_to_' in market_key:
                market_lower = market.lower()
                is_no = (
                    "doesn't" in market_lower or "doesnt" in market_lower
                    or (market_key == 'fight_to_go_the_distance' and 'ends inside distance' in market_lower)
                )
                outcome_name = 'No' if is_no else 'Yes'
                outcome_description = None
                outcome_point = None
            elif 'player_to_win_' in market_key:
                outcome_name = 'No' if ("doesn't" in market.lower() or "doesnt" in market.lower()) else 'Yes'
                outcome_description = _player_outcome_description(row)
                outcome_point = None
            else:
                outcome_name = None
                outcome_description = None
                outcome_point = None

            return outcome_name, outcome_description, outcome_point

        results = self.df.apply(_row_transform, axis=1)
        self.df['outcome_name'] = [r[0] for r in results]
        self.df['outcome_description'] = [r[1] for r in results]
        self.df['outcome_point'] = [r[2] for r in results]
        return self.df

    # Odds/bookmaker columns to clear when creating placeholder rows (no odds)
    _ODDS_COLUMNS = {
        'bovada', 'pinnacle', 'draftkings', 'fanduel', 'polymarket', 'betrivers',
        'hardrockbet', 'betmgm', 'caesars', 'pointsbet',
        'highest_bettable_odds', 'average_bettable_odds', 'sportsbooks_used', 'odds',
    }

    def ensure_player_to_win_by_submission_no_rows(self):
        """For each fight, ensure there is a row for player_to_win_by_submission with outcome No
        for each fighter. If missing, duplicate a row from that fight and set market_key, outcome_*,
        and clear odds. New rows are inserted after the last row of each fight (so they appear
        with their fight, not at the end). Call after format_outcome_description_name_point, before format_merge_outcome_wagers."""
        if 'game_id' not in self.df.columns or 'market_key' not in self.df.columns:
            return self.df

        valid = self.df[self.df['game_id'].notna() & (self.df['game_id'].astype(str).str.strip() != '')]
        if valid.empty:
            return self.df

        existing = self.df[
            (self.df['market_key'] == 'player_to_win_by_submission') &
            (self.df['outcome_name'] == 'No')
        ]
        existing_pairs = set(
            zip(
                existing['game_id'].astype(str).str.strip(),
                existing['outcome_description'].fillna('').astype(str).str.strip()
            )
        )

        # Build (insert_after_index, new_row) so we insert No rows right after each fight's last row
        inserts = []  # list of (index_after_which_to_insert, list of row dicts)
        for game_id, group in valid.groupby('game_id'):
            game_id_str = str(game_id).strip()
            first = group.iloc[0]
            last_idx = group.index[-1]
            home = str(first.get('home_team', '') or '').strip()
            away = str(first.get('away_team', '') or '').strip()
            for fighter in (home, away):
                if not fighter:
                    continue
                if (game_id_str, fighter) in existing_pairs:
                    continue
                row = first.to_dict()
                row['market'] = f"{fighter} doesn't win by submission"
                row['market_key'] = 'player_to_win_by_submission'
                row['outcome_name'] = 'No'
                row['outcome_description'] = fighter
                row['outcome_point'] = None
                for col in self._ODDS_COLUMNS:
                    if col in row:
                        row[col] = pd.NA
                inserts.append((last_idx, row))

        if not inserts:
            return self.df

        # Insert in reverse order of index so positions don't shift
        inserts_by_idx = {}
        for idx, row in inserts:
            inserts_by_idx.setdefault(idx, []).append(row)

        pieces = []
        last_end = 0
        for idx in sorted(inserts_by_idx.keys()):
            pieces.append(self.df.iloc[last_end : idx + 1])
            extra = pd.DataFrame(inserts_by_idx[idx])
            extra = extra.reindex(columns=self.df.columns)
            pieces.append(extra)
            last_end = idx + 1
        pieces.append(self.df.iloc[last_end:])
        self.df = pd.concat(pieces, ignore_index=True)
        return self.df

    def format_merge_outcome_wagers(self):
        """Build merge_column_key, merge_column_key_other, outcome, and wagers columns."""
        other_over_under = {'Over': 'Under', 'Under': 'Over', 'Yes': 'No', 'No': 'Yes'}

        def _other_team(outcome_name, home, away):
            if outcome_name == home:
                return away
            if outcome_name == away:
                return home
            return outcome_name

        def _row_transform(row):
            game_id = str(row.get('odds_api_game_id', '') or '')
            market_key = str(row.get('market_key', '') or '')
            outcome_name = row.get('outcome_name')
            outcome_name = '' if pd.isna(outcome_name) or outcome_name is None else str(outcome_name)
            home = str(row.get('home_team', '') or '')
            away = str(row.get('away_team', '') or '')
            outcome_desc = row.get('outcome_description')
            outcome_pt = row.get('outcome_point')
            outcome_desc_s = '' if pd.isna(outcome_desc) or outcome_desc is None else str(outcome_desc)
            outcome_pt_s = '' if pd.isna(outcome_pt) or outcome_pt is None else str(outcome_pt)

            # merge_column_key and merge_column_key_other
            if market_key == 'h2h':
                merge_key = f"{game_id}_{market_key}_{outcome_name}"
                merge_other = f"{game_id}_{market_key}_{_other_team(outcome_name, home, away)}"
            elif 'totals' in market_key and 'team' not in market_key:
                merge_key = f"{game_id}_{market_key}_{outcome_name}_{outcome_pt_s}"
                merge_other = f"{game_id}_{market_key}_{other_over_under.get(outcome_name, outcome_name)}_{outcome_pt_s}"
            elif outcome_name in ('Yes', 'No'):
                merge_key = f"{game_id}_{market_key}_{outcome_desc_s}_{outcome_name}"
                merge_other = f"{game_id}_{market_key}_{outcome_desc_s}_{other_over_under[outcome_name]}"
            else:
                merge_key = f"{game_id}_{market_key}_{outcome_desc_s}_{outcome_name}_{outcome_pt_s}"
                merge_other = f"{game_id}_{market_key}_{outcome_desc_s}_{other_over_under.get(outcome_name, outcome_name)}_{outcome_pt_s}"

            # outcome column
            outcome = f"{market_key}_{outcome_name}"
            if outcome_desc_s:
                outcome += f"_{outcome_desc_s}"
            if outcome_pt_s:
                outcome += f"_{outcome_pt_s}"

            # wagers column
            if not outcome_pt_s:
                wagers = str(outcome_name) if outcome_name else ''
            elif not outcome_desc_s:
                wagers = f"{outcome_name}_{outcome_pt_s}"
            else:
                wagers = f"{outcome_desc_s}_{outcome_name}_{outcome_pt_s}"
            if wagers.endswith('_'):
                wagers = wagers[:-1]

            return merge_key, merge_other, outcome, wagers

        results = self.df.apply(_row_transform, axis=1)
        self.df['merge_column_key'] = [r[0] for r in results]
        self.df['merge_column_key_other'] = [r[1] for r in results]
        self.df['outcome'] = [r[2] for r in results]
        self.df['wagers'] = [r[3] for r in results]
        return self.df

    def _wagers_to_display(self, wagers_str):
        """Convert wagers string to display format (Over +1.5, Team -3.5, etc)."""
        if not wagers_str:
            return ''
        parts = wagers_str.split('_')
        if len(parts) == 1:
            return wagers_str
        elif len(parts) == 2:
            if parts[1] and parts[1][0] != '-':
                return f'{parts[0]} +{parts[1]}'
            return f'{parts[0]} {parts[1]}'
        elif len(parts) == 3:
            if 'over' in parts[0].lower() or 'under' in parts[0].lower():
                return f'{parts[0]} {parts[1]}'
            if 'over' in parts[1].lower() or 'under' in parts[1].lower():
                return f'{parts[0]} {parts[1]} {parts[2]}'
        return ' '.join(parts)

    def format_market_display(self):
        """Map market_key to market_display for MMA markets."""
        mma_market_display = {
            'h2h': 'Moneyline',
            'totals': 'Game Total',
            'alternate_totals': 'Alternate Game Total',
            'player_to_win_by_submission': 'Fighter To Win by Submission',
            'player_to_win_by_ko': 'Fighter To Win by KO',
            'fight_to_go_the_distance': 'Fight To Go the Distance',
            'player_to_win_by_decision': 'Fighter To Win by Decision',
            'fight_to_end_by_submission': 'Fight To End by Submission',
            'player_to_win_by_ko_or_tko_or_decision': 'Fighter To Win by KO/TKO/Decision',
            'player_to_win_by_ko_or_tko_or_submission': 'Fighter To Win by KO/TKO/Submission',
            'player_to_win_by_submission_or_decision': 'Fighter To Win by Submission or Decision',
            'player_to_win_by_ko_or_tko_or_dq_round_1': 'Fighter To Win in Round 1 by KO/TKO/DQ',
            'player_to_win_by_ko_or_tko_or_dq_round_2': 'Fighter To Win in Round 2 by KO/TKO/DQ',
            'player_to_win_by_ko_or_tko_or_dq_round_3': 'Fighter To Win in Round 3 by KO/TKO/DQ',
            'player_to_win_by_ko_or_tko_or_dq_round_4': 'Fighter To Win in Round 4 by KO/TKO/DQ',
            'player_to_win_by_ko_or_tko_or_dq_round_5': 'Fighter To Win in Round 5 by KO/TKO/DQ',
            'player_to_win_by_submission_round_1': 'Fighter To Win in Round 1 by Submission',
            'player_to_win_by_submission_round_2': 'Fighter To Win in Round 2 by Submission',
            'player_to_win_by_submission_round_3': 'Fighter To Win in Round 3 by Submission',
            'player_to_win_by_submission_round_4': 'Fighter To Win in Round 4 by Submission',
            'player_to_win_by_submission_round_5': 'Fighter To Win in Round 5 by Submission',
            'fight_to_end_by_ko_or_tko_or_dq': 'Fight To End by KO/TKO/DQ',
            'player_to_win_by_ko_or_tko': 'Fighter To Win by KO/TKO',
            'player_to_win_in_round_1': 'Fighter To Win in Round 1',
            'player_to_win_in_round_2': 'Fighter To Win in Round 2',
            'player_to_win_in_round_3': 'Fighter To Win in Round 3',
            'player_to_win_in_round_4': 'Fighter To Win in Round 4',
            'player_to_win_in_round_5': 'Fighter To Win in Round 5',
        }
        self.df['market_display'] = self.df['market_key'].map(mma_market_display)
        return self.df

    def format_wager_display(self):
        """Build wager_display from wagers column."""
        self.df['wager_display'] = self.df['wagers'].apply(
            lambda x: self._wagers_to_display(str(x) if pd.notna(x) else '')
        )
        self.df.columns = [str(c).lower() for c in self.df.columns]
        return self.df

    def format_snapshot_time(self):
        """Add snapshot_time column with current UTC time (format: 2026-02-23 21:23:14)."""
        self.df['snapshot_time'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        return self.df

    def format_cleanup(self):
        """Drop rows with null outcome/game_id, drop odds column, add cached_links."""
        self.df.to_csv('before_cleanup.csv', index=False)
        self.df = self.df[self.df['outcome'].notna()]
        print("After dropping rows with null outcome: ", len(self.df))
        self.df = self.df[self.df['game_id'].notna()]
        print("After dropping rows with null game_id: ", len(self.df))
        self.df = self.df[self.df['market_key'].notna()]
        print("After dropping rows with null market_key: ", len(self.df))
        self.df = self.df[self.df['market_key'] != '']
        print("After dropping rows with empty market_key: ", len(self.df))
        # Drop totals/alternate_totals rows with null outcome_name
        totals_mask = self.df['market_key'].isin(['totals', 'alternate_totals'])
        null_outcome = self.df['outcome_name'].isna()
        self.df = self.df[~(totals_mask & null_outcome)]
        print("After dropping rows with null outcome_name: ", len(self.df))
        self.df = self.df.drop(columns=['odds'], errors='ignore')
        print("After dropping odds column: ", len(self.df))
        self.df['cached_links'] = ''
        print("After adding cached_links column: ", len(self.df))
        return self.df

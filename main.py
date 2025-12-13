import math
import time
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from enum import Enum
from typing import NamedTuple, Dict, TypedDict, Literal, Union, List
import asyncio
import aiohttp
import json
from datetime import datetime, timezone

# import yappi
app = FastAPI()

def to_timestamp(timestamp_str):
    try:
        dt = datetime.strptime(timestamp_str, "%m/%d/%Y %I:%M:%S %p")
        if dt.year < 1970:
            return 0
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception as e:
        print(e)
        return 0

class GameSession(TypedDict):
    authenticated: bool
    sessionstarted: bool
    twitchusername: str
    players: int
    gameversion: str
    secondssincestart: float

class GameMultiplier(TypedDict):
    eventname: str
    active: bool
    multiplier: float
    elapsed: float
    duration: float
    timeleft: float
    starttime: str
    endtime: str

class Boss(TypedDict):
    health: int
    maxhealth: int
    healthpercent: int
    combatlevel: int
    
class Dungeon(TypedDict):
    started: bool
    secondsuntilstart: int
    name: str
    room: int
    players: int
    playersalive: int
    enemies: int
    enemiesalive: int
    elapsed: float
    count: int
    boss: Boss

class Raid(TypedDict):
    started: bool
    players: int
    timeleft: float
    count: int
    boss: Boss
    
class PlayerStat(TypedDict):
    level: int
    currentvalue: int
    maxlevel: int
    experience: float

class Player(TypedDict):
    id: str
    name: str
    training: str
    taskargument: str
    island: str
    sailing: bool
    resting: bool
    restedtime: float
    inarena: bool
    induel: bool
    indungeon: bool
    inraid: bool
    coins: int
    commandidletime: float
    stats: Dict[Literal[
        "combatlevel", "attack", "defense", "strength", "health", "woodcutting",
        "fishing", "mining", "crafting", "cooking", "farming", "slayer", "magic", "ranged",
        "sailing", "healing", "gathering", "alchemy"
    ], Union[int, PlayerStat]]
    town: str

class Village(TypedDict):
    name: str
    level: int
    tier: int
    boost: str

class FerryCaptain(TypedDict):
    name: str
    sailinglevel: int

class Ferry(TypedDict):
    destination: str
    players: int
    captain: FerryCaptain


MAX_LEVEL = 999
experience_array = [0] * MAX_LEVEL

exp_for_level = 100
for level_index in range(MAX_LEVEL):
    level = level_index + 1
    tenth = math.trunc(level / 10) + 1
    incrementor = tenth * 100 + math.pow(tenth, 3)
    exp_for_level += math.trunc(incrementor)
    experience_array[level_index] = exp_for_level

def experience_for_level(level):
    if level - 2 >= len(experience_array):
        return experience_array[len(experience_array) - 1]
    return (0 if level - 2 < 0 else experience_array[level - 2])


class UndefinedMetric(Exception):
    def __init__(self):
        super().__init__("bruh")

class AlreadyExists(Exception):
    def __init__(self):
        super().__init__("bruh")

class MetricType(Enum):
    COUNTER = 0
    GAUGE = 1

class MetricDefinition(NamedTuple):
    name: str
    description: str
    metric_type: MetricType

class MetricEntry(NamedTuple):
    name: str
    labels: str

ababab = str.maketrans({
    '"': '\\"',
    '\\': '\\\\',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
})
def to_label(obj):
    if isinstance(obj, bool):
        return "true" if obj else "false"
    elif isinstance(obj, str):
        return obj.translate(ababab)
    else:
        return str(obj)

class Metrics:
    def __init__(self):
        self.definitions: Dict[str, MetricDefinition] = {}
        self.metrics: Dict[MetricEntry, float] = {}
        
    def add_value(self, metric_name: str, value: float | int | bool, **labels):
        if value is None:
            print(f"Empty metric: {metric_name}, {labels}")
            return
        # b = ','.join([f'{x}=\"{json.dumps(y).strip('"')}\"' for x, y in labels.items()])
        b = ','.join([f'{x}=\"{to_label(y)}\"' for x, y in labels.items()])
                      
        a = MetricEntry(metric_name, b)
        # if a in self.metrics:
        #     raise AlreadyExists()
        # if not a.name in self.definitions:
        #     raise UndefinedMetric()
        if isinstance(value, bool):
            self.metrics[a] = 1 if value else 0
        self.metrics[a] = float(value)
    
    def add_def(self, metric_name: str, description: str, type: MetricType=MetricType.GAUGE, *, value: float | int | bool = None, **labels):
        # if metric_name in self.definitions:
        #     raise AlreadyExists()
        self.definitions[metric_name] = MetricDefinition(metric_name, description, type)
        if value is not None:
            self.add_value(metric_name, value, **labels)
    
    def get_text(self):
        # metrics = sorted(list(self.metrics.keys()), key=lambda x: x.name)
        metrics = list(self.metrics.keys())
        defs = set(self.definitions.keys())
        out_text = []
        for m in metrics:
            if m.name in defs:
                metric_def = self.definitions[m.name]
                out_text.extend([
                    f"# HELP {m.name} {metric_def.description}",
                    f"# TYPE {m.name} {metric_def.metric_type.name.lower()}",
                ])
                defs.remove(m.name)
            value = self.metrics[m]
            labels = m.labels
            if labels:
                labels = "{%s}" % labels
            out_text.append(
                f"{m.name}{labels} {value}"
            )
        return "\n".join(out_text)

async def fetch(session: aiohttp.ClientSession, url):
    async with session.get(url) as response:
        if response.status == 200:
            try:
                return await response.json()
            except aiohttp.ContentTypeError:
                return None
        else:
            return None

with open("./servers.json", "r") as f:
    SERVERS = json.load(f)

dungeon_healths = {}
raid_durations = {}
@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    # yappi.set_clock_type("wall") # Use set_clock_type("wall") for wall time
    # yappi.start()

    urls = []
    requests_per_server = 7
    for thing in SERVERS:
        thing: str
        thing = thing.rstrip("/")
        urls.extend([
            f"{thing}/select * from session",
            f"{thing}/select * from village",
            f"{thing}/select * from dungeon",
            f"{thing}/select * from multiplier", 
            f"{thing}/select * from raid",
            f"{thing}/select * from ferry",
            f"{thing}/select * from players",
        ])

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
    instance_data = [results[i:i + requests_per_server] for i in range(0, len(results), requests_per_server)]
    # t0 = time.monotonic()
    m = Metrics()
    for data in instance_data:
        if None in data:
            continue
        session_, village, dungeon, multiplier, raid, ferry, players = data
        session_: GameSession
        village: Village
        dungeon: Dungeon
        multiplier: GameMultiplier
        raid: Raid
        ferry: Ferry
        players: List[Player]
        
        if not isinstance(session_, dict):
            continue
        if not session_.get("twitchusername"):
            continue
        
        labels = {"session": session_.get("twitchusername", "")}
        m.add_def("rf_session_info", "Textual info about the game session")
        m.add_value("rf_session_info", 1, username=session_.get("twitchusername", ""), game_version=session_.get('gameversion', ""), **labels)
        m.add_def("rf_session_authenticated", "Session is authenticated")
        m.add_value("rf_session_authenticated", session_.get('authenticated', False), **labels)
        m.add_def("rf_session_started", "Session has started")
        m.add_value("rf_session_started", session_.get('sessionstarted', False), **labels)
        m.add_def("rf_session_duration_seconds", "Time elapsed since game startup", MetricType.COUNTER)
        m.add_value("rf_session_duration_seconds", session_.get('secondssincestart'), **labels)
        m.add_def("rf_players_in_session", "Number of players in the session", value=session_.get('players'), **labels)
        
        if isinstance(dungeon, dict):
            m.add_def("rf_dungeon_info", "Textual info about the dungeon")
            m.add_value("rf_dungeon_info", 1, name=dungeon.get('name', ''), **labels)
            m.add_def("rf_dungeon_started", "Dungeon has started")
            m.add_value("rf_dungeon_started", dungeon.get('started'), **labels)
            m.add_def("rf_dungeon_time_until_start_seconds", "Seconds remaining until dungeon starts", value=dungeon.get('secondsuntilstart'), **labels)
            m.add_def("rf_dungeon_current_room", "Current room number", value=dungeon.get('room'), **labels)
            m.add_def("rf_dungeon_players_total", "Total number of participating players", value=dungeon.get('players'), **labels)
            m.add_def("rf_dungeon_players_alive_total", "Number of players fighting in the dungeon", value=dungeon.get('playersalive'), **labels)
            m.add_def("rf_dungeon_enemies_total", "Total number of enemies in the dungeon", value=dungeon.get('enemies'), **labels)
            m.add_def("rf_dungeon_enemies_alive_total", "Number of alive enemies", value=dungeon.get('enemiesalive'), **labels)
            m.add_def("rf_dungeon_duration_seconds", "Seconds since dungeon start", value=dungeon.get('secondsuntilstart'), **labels)
            m.add_def("rf_dungeons_total", "Number of dungeons since game start", MetricType.COUNTER, value=dungeon.get('count'), **labels)
            m.add_def("rf_dungeon_boss_health", "Current HP value of the dungeon boss", value=dungeon.get('boss', {}).get('health'), **labels)
            m.add_def("rf_dungeon_boss_combat_level", "Combat level of the dungeon boss", value=dungeon.get('boss', {}).get('combatlevel'), **labels)
            if dungeon and 'enemies' in dungeon:
                if not dungeon.get('started'):
                    dungeon_healths[session_.get("twitchusername")] = dungeon.get('boss', {}).get('health')
                else:
                    if dungeon.get('enemiesalive') > 0 or not session_.get("twitchusername") in dungeon_healths:
                        dungeon_healths[session_.get("twitchusername")] = dungeon.get('boss', {}).get('health')
            m.add_def("rf_dungeon_boss_max_health", "Maximum health of the dungeon boss", value=dungeon_healths.get(session_.get("twitchusername")), **labels)
        
        if isinstance(multiplier, dict):
            m.add_def("rf_multiplier_info", "Textual info about the multiplier", value=1, event_name=multiplier.get('eventname'), **labels)
            m.add_def("rf_multiplier_active", "Multiplier is active", value=multiplier.get('active'), **labels)
            m.add_def("rf_multiplier_elapsed_time_seconds", "Seconds since multiplier event started", value=multiplier.get('elapsed'), **labels)
            m.add_def("rf_multiplier_duration_seconds", "Duration of multiplier in seconds", value=multiplier.get('duration'), **labels)
            mult_start_ts = None
            mult_end_ts = None
            if "starttime" in multiplier:
                mult_start_ts = to_timestamp(multiplier['starttime'])
            if "endtime" in multiplier:
                mult_end_ts = to_timestamp(multiplier["endtime"])
            m.add_def("rf_multiplier_start_timestamp_seconds", "Start timestamp of multiplier", value=mult_start_ts, **labels)
            m.add_def("rf_multiplier_end_timestamp_seconds", "End timestamp of multiplier", value=mult_end_ts, **labels)
            m.add_def("rf_multiplier_value", "Current multiplier factor", value=multiplier.get('multiplier'), **labels)
        
        if isinstance(raid, dict):
            m.add_def("rf_raid_started", "Raid has started", value=raid.get('started'), **labels)
            m.add_def("rf_raid_players_total", "Number of players participating", value=raid.get('players'), **labels)
            m.add_def("rf_raid_time_remaining_seconds", "Time left until raid fails", value=raid.get('timeleft'), **labels)
            m.add_def("rf_raid_total", "Number of raids since game start", MetricType.COUNTER, value=raid.get('count'), **labels)
            m.add_def("rf_raid_boss_health", "Current health of the raid boss", value=raid.get('boss', {}).get('health'), **labels)
            m.add_def("rf_raid_boss_max_health", "Maximum health of the raid boss", value=raid.get('boss', {}).get('maxhealth'), **labels)
            m.add_def("rf_raid_boss_combat_level", "Combat level of the raid boss", value=raid.get('boss', {}).get('combatlevel'), **labels)
        
        if isinstance(ferry, dict):
            m.add_def(
                "rf_ferry_info", "Textual information about the ferry", value=1, 
                destination=ferry.get('destination'), captain=ferry.get('captain', {}).get('name', ''), **labels
            )
            m.add_def("rf_ferry_players_total", "Number of players on the ferry", value=ferry.get('players'), **labels)
            m.add_def("rf_ferry_captain_sailing_skill_level", "Sailing level of the captain", value=ferry.get('captain', {}).get('sailinglevel'), **labels)
        
        m.add_def("rf_player_info", "Info about players in the session")
        m.add_def("rf_player_stat_base_level", "Current base level of stat", MetricType.COUNTER)
        m.add_def("rf_player_stat_max_level", "Current level with enchantments of stat")
        m.add_def("rf_player_stat_value", "Current value of stat (only relevant for the Health stat)")
        m.add_def("rf_player_stat_experience_since_last_level_up", "Experience gained since last level up for stat", MetricType.COUNTER)
        m.add_def("rf_player_stat_experience_total", "Total experience points gained for stat", MetricType.COUNTER)
        
        m.add_def("rf_player_rested_time_seconds", "Rested time of the player in seconds")
        m.add_def("rf_player_command_last_execution_time_seconds", "Time since the player last executed a command")
        m.add_def("rf_player_coins_total", "Number of coins the player has")
        m.add_def("rf_player_combat_level", "Combat level of the player", MetricType.COUNTER)

        resting_count = 0
        total_experience_by_skill = {}
        total_experience_by_session = 0
        if isinstance(players, (list, tuple)):
            for player in players:
                if not isinstance(player, dict):
                    continue
                m.add_value(
                    "rf_player_info", 1,
                    id=player.get("id"),
                    name=player.get('name'),
                    training=player.get('training'),
                    task_argument=player.get('taskargument'),
                    island=player.get('island'),
                    sailing=player.get('sailing'),
                    resting=player.get('resting'),
                    rested_time=player.get('restedtime'),
                    in_arena=player.get('inarena'),
                    in_duel=player.get('induel'),
                    in_dungeon=player.get('indungeon'),
                    in_raid=player.get('inraid'),
                    coins=player.get('coins'),
                    **labels
                )
                
                if player.get('resting'):
                    resting_count += 1
                
                player_labels = {
                    "player_name": player.get("name"),
                    "player_id": player.get("id"),
                }

                m.add_value("rf_player_rested_time_seconds", player.get('restedtime'), **player_labels, **labels)
                m.add_value("rf_player_command_last_execution_time_seconds", player.get('commandidletime'), **player_labels, **labels)
                m.add_value("rf_player_coins_total", player.get('coins'), **player_labels, **labels)
                m.add_value("rf_player_combat_level", player.get('stats', {}).get('combatlevel'), **player_labels, **labels)

                for stat_name, stat_info in player.get('stats', {}).items():
                    if stat_name == "combatlevel":
                        continue
                    m.add_value(
                        f"rf_player_stat_base_level", 
                        value=stat_info.get('level'), **player_labels, **labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_max_level", 
                        value=stat_info.get('maxlevel'), **player_labels, **labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_value", 
                        value=stat_info.get('currentvalue'), **player_labels, **labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_experience_since_last_level_up", 
                        value=stat_info.get('experience'), **player_labels, **labels, stat=stat_name
                    )
                    cumulative_exp = stat_info.get('experience')
                    for x in range(1,stat_info.get('level', 0)+1):
                        cumulative_exp += experience_for_level(x)
                    m.add_value(
                        f"rf_player_stat_experience_total", 
                        value=cumulative_exp, **player_labels, **labels, stat=stat_name
                    )
                    if not stat_name in total_experience_by_skill:
                        total_experience_by_skill[stat_name] = 0
                    total_experience_by_skill[stat_name] += cumulative_exp
                    total_experience_by_session += cumulative_exp
        m.add_def("rf_session_resting_players_total", "Total number of players currently resting", MetricType.COUNTER, value=resting_count, **labels)
        m.add_def("rf_session_experience_total", "Sum of all experience of all players in the session", value=total_experience_by_session, **labels)
        m.add_def("rf_session_experience_by_skill_total", "Sum of all experience of all players in the session categorized by skill")
        for skill_name, total_exp in total_experience_by_skill.items():
            m.add_value(
                "rf_session_experience_by_skill_total", total_exp, stat=skill_name, **labels
            )
        
        if isinstance(village, dict):
            boost_stats = []
            for a in village.get('boost', '').split(', '):
                split = a.split(' ', maxsplit=1)
                if len(split) == 2:
                    stat, percent = split
                else:
                    continue
                boost_stats.append(stat)
                m.add_def(
                    "rf_village_boost_percent", "Village boost percentage",
                    value=float(percent.strip("%")), **labels, stat=stat
                )
            boost_stats_text = ','.join(boost_stats)
            m.add_def("rf_village_info", "Textual info about the village", value=1, name=village.get('name'), boost=village.get('boost'), boost_stats=boost_stats_text, **labels)
            m.add_def("rf_village_tier_level", "Village tier level", value=village.get('tier'), **labels)
            m.add_def("rf_village_level", "Village level", value=village.get('level'), **labels)
    # t1 = time.monotonic()
    # print((t1-t0)*1000)
    # t0 = time.monotonic()
    text = m.get_text()
    # t1 = time.monotonic()
    # print((t1-t0)*1000)
    # yappi.get_func_stats().print_all()
    # yappi.get_thread_stats().print_all()
    return text

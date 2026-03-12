import math
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from collections.abc import ItemsView, Mapping
from enum import Enum
from typing import NamedTuple, TypedDict, Any, cast, TypeGuard
import asyncio
import aiohttp
import json
from datetime import datetime, timezone
import logging
from pydantic import TypeAdapter, ValidationError
from enum import StrEnum

mainlogger = logging.getLogger("main")
logger = logging.getLogger("validation")
logger.setLevel(logging.ERROR)
handler = logging.FileHandler("validation_errors.log")
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

app = FastAPI()

def to_timestamp(timestamp_str: str):
    try:
        dt = datetime.strptime(timestamp_str, "%m/%d/%Y %I:%M:%S %p")
        if dt.year < 1970:
            return 0
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception as e:
        print(e)
        return 0

# select * from session
class GameSession(TypedDict):
    authenticated: bool
    sessionstarted: bool
    twitchusername: str | None
    players: int
    gameversion: str
    secondssincestart: float

# select * from multiplier
class GameMultiplier(TypedDict):
    eventname: str | None
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
    healthpercent: float
    combatlevel: int

# select * from dungeon
class Dungeon(TypedDict):
    started: bool
    secondsuntilstart: float
    name: str
    room: int
    players: int
    playersalive: int
    enemies: int
    enemiesalive: int
    elapsed: float
    count: int
    boss: Boss

# select * from raid
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

class PlayerStats(TypedDict):
    combatlevel: int
    attack: PlayerStat
    defense: PlayerStat
    strength: PlayerStat
    health: PlayerStat
    woodcutting: PlayerStat
    fishing: PlayerStat
    mining: PlayerStat
    crafting: PlayerStat
    cooking: PlayerStat
    farming: PlayerStat
    slayer: PlayerStat
    magic: PlayerStat
    ranged: PlayerStat
    sailing: PlayerStat
    healing: PlayerStat
    gathering: PlayerStat
    alchemy: PlayerStat

# select * from players
# select * from observed
class Player(TypedDict):
    id: str
    name: str
    training: str
    taskargument: str | None
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
    stats: PlayerStats


# select * from village
class Village(TypedDict):
    name: str
    level: int
    tier: int
    boost: str


class FerryCaptain(TypedDict):
    name: str
    sailinglevel: int

class FerryBoost(TypedDict):
    isactive: bool
    remainingtime: float

# select * from ferry
class Ferry(TypedDict):
    destination: str
    players: int
    captain: FerryCaptain
    boost: FerryBoost


class IslandLevels(TypedDict):
    skill: int
    combat: int

class IslandName(StrEnum):
    HOME = "Home"
    AWAY = "Away"
    IRONHILL = "Ironhill"
    KYO = "Kyo"
    HEIM = "Heim"
    ATRIA = "Atria"
    ELDARA = "Eldara"
    WAR = "War"

# select * from islands
class Island(TypedDict):
    name: IslandName
    players: int
    level: IslandLevels

# select * from redeemables
class Redeemable(TypedDict):
    itemid: str
    name: str
    description: str | None
    currency: str
    cost: int


class GameSettings(TypedDict):
    playercacheexpirytimeindex: int
    camerarotationspeed: int
    daynighttime: int
    daynightcycleenabled: bool
    realtimedaynightcycle: bool
    autokickafkplayers: bool
    localbotserverdisabled: bool
    alertexpiredstatecacheinchat: bool
    canobserveemptyislands: bool
    playerboostrequirement: int
    itemdropmessagetype: int
    pathfindingqualitysettings: int
    localbotport: int
    islandobserveseconds: int

class SoundSettings(TypedDict):
    musicvolume: int
    raidhornvolume: int

class UISettings(TypedDict):
    playernamesvisible: bool
    playerlistsize: int
    playerlistscale: float

class GraphicsSettings(TypedDict):
    qualitylevel: int
    dpiscale: int
    potatomode: bool
    autopotatomode: bool
    postprocessing: bool

class QueryEngineSettings(TypedDict):
    enabled: bool
    alwaysreturnarray: bool
    apiprefix: str

class StreamLabelsSettings(TypedDict):
    enabled: bool
    savetextfiles: bool
    savejsonfiles: bool

class PlayerObserveSeconds(TypedDict):
    default: int
    subscriber: int
    moderator: int
    vip: int
    broadcaster: int
    onsubcription: int
    oncheeredbits: int

class LootSettings(TypedDict):
    includeorigin: bool

# select * from settings
class GameConfig(TypedDict):
    game: GameSettings
    sound: SoundSettings
    ui: UISettings
    graphics: GraphicsSettings
    queryengine: QueryEngineSettings
    streamlabels: StreamLabelsSettings
    playerobserveseconds: PlayerObserveSeconds
    loot: LootSettings


game_session_adapter = TypeAdapter(GameSession)
village_adapter = TypeAdapter(Village)
dungeon_adapter = TypeAdapter(Dungeon)
multiplier_adapter = TypeAdapter(GameMultiplier)
raid_adapter = TypeAdapter(Raid)
ferry_adapter = TypeAdapter(Ferry)
player_list_adapter = TypeAdapter(list[Player])


MAX_LEVEL = 999
experience_array = [0] * MAX_LEVEL

exp_for_level = 100
for level_index in range(MAX_LEVEL):
    level = level_index + 1
    tenth = math.trunc(level / 10) + 1
    incrementor = tenth * 100 + math.pow(tenth, 3)
    exp_for_level += math.trunc(incrementor)
    experience_array[level_index] = exp_for_level

def experience_for_level(level: int) -> int:
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
def to_label(obj: object):
    if isinstance(obj, bool):
        return "true" if obj else "false"
    elif isinstance(obj, str):
        return obj.translate(ababab)
    else:
        return str(obj)

class Metrics:
    def __init__(self):
        self.definitions: dict[str, MetricDefinition] = {}
        self.metrics: dict[MetricEntry, float] = {}
        
    def add_value(self, metric_name: str, value: float | int | bool, label_dict: Mapping[str, object] | None = None, **labels: str | object):
        if label_dict is None:
            label_dict = {}
        l_a = [f'{x}=\"{to_label(y)}\"' for x, y in labels.items()]
        l_b = [f'{x}=\"{to_label(y)}\"' for x, y in label_dict.items()]
        b = ','.join(l_a + l_b)
                      
        a = MetricEntry(metric_name, b)
        if isinstance(value, bool):
            self.metrics[a] = 1 if value else 0
        self.metrics[a] = float(value)
    
    def add_def(self, metric_name: str, description: str, type: MetricType=MetricType.GAUGE, *, value: float | int | bool | None = None, label_dict: Mapping[str, object] | None = None, **labels: str | object):
        self.definitions[metric_name] = MetricDefinition(metric_name, description, type)
        if value is not None:
            self.add_value(metric_name, value, label_dict=label_dict, **labels)
    
    def get_text(self):
        metrics: list[MetricEntry] = list(self.metrics.keys())
        defs = set(self.definitions.keys())
        out_text: list[str] = []
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

class QueryException(BaseException):
    pass

async def fetch(session: aiohttp.ClientSession, url: str) -> Any | None:
    async with session.get(url) as response:
        if response.status == 200:
            try:
                response = await response.json()
                if isinstance(response, dict):
                    if not response:
                        return None
                    if "error" in response:
                        raise QueryException(response['error'])
                return response
            except aiohttp.ContentTypeError:
                return None
        else:
            return None

with open("./servers.json", "r") as f:
    SERVERS = cast(list[str], json.load(f))

dungeon_healths: dict[str, int] = {}
raid_durations: dict[str, float] = {}
@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    # yappi.set_clock_type("wall") # Use set_clock_type("wall") for wall time
    # yappi.start()

    urls: list[str] = []
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

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    instance_data = [results[i:i + requests_per_server] for i in range(0, len(results), requests_per_server)]
    # t0 = time.monotonic()
    m = Metrics()
    def validate_data[T](data_to_validate: Any, adapter: TypeAdapter[T], name: str) -> TypeGuard[T]:
        if data_to_validate is not None and isinstance(data_to_validate, (dict, list)):
            try:
                _ = adapter.validate_python(data_to_validate, strict=True)
                return True
            except ValidationError as e:
                mainlogger.error("Validation error for %s: %s", name, e)
                logger.error("Validation error for %s: %s\n Data: %s", name, e, data_to_validate)
                return False
        return False

    for data in instance_data:
        data_f: list[dict[str, Any] | list[Player] | None] = []
        for d in data:
            if isinstance(d, BaseException):
                data_f.append(None)
            else:
                data_f.append(d)
        session_, village, dungeon, multiplier, raid, ferry, players = data_f  # pyright: ignore[reportAssignmentType]
        session_: GameSession | None
        village: Village | None
        dungeon: Dungeon | None
        multiplier: GameMultiplier | None
        raid: Raid | None
        ferry: Ferry | None
        players: Player | list[Player] | None

        
        if not validate_data(session_, game_session_adapter, "GameSession"):
            continue
        if not session_.get("twitchusername"):
            continue
        
        labels: dict[str, str] = {"session": session_.get("twitchusername", "")}
        m.add_def("rf_session_info", "Textual info about the game session")
        m.add_value("rf_session_info", 1, username=session_.get("twitchusername", ""), game_version=session_.get('gameversion', ""), label_dict=labels)
        m.add_def("rf_session_authenticated", "Session is authenticated")
        m.add_value("rf_session_authenticated", session_.get('authenticated', False), label_dict=labels)
        m.add_def("rf_session_started", "Session has started")
        m.add_value("rf_session_started", session_.get('sessionstarted', False), label_dict=labels)
        m.add_def("rf_session_duration_seconds", "Time elapsed since game startup", MetricType.COUNTER)
        m.add_value("rf_session_duration_seconds", session_.get('secondssincestart'), label_dict=labels)
        m.add_def("rf_players_in_session", "Number of players in the session", value=session_.get('players'), label_dict=labels)
        
        if validate_data(dungeon, dungeon_adapter, "Dungeon"):
            m.add_def("rf_dungeon_info", "Textual info about the dungeon")
            m.add_value("rf_dungeon_info", 1, name=dungeon.get('name', ''), label_dict=labels)
            m.add_def("rf_dungeon_started", "Dungeon has started")
            m.add_value("rf_dungeon_started", dungeon.get('started'), label_dict=labels)
            m.add_def("rf_dungeon_time_until_start_seconds", "Seconds remaining until dungeon starts", value=dungeon.get('secondsuntilstart'), label_dict=labels)
            m.add_def("rf_dungeon_current_room", "Current room number", value=dungeon.get('room'), label_dict=labels)
            m.add_def("rf_dungeon_players_total", "Total number of participating players", value=dungeon.get('players'), label_dict=labels)
            m.add_def("rf_dungeon_players_alive_total", "Number of players fighting in the dungeon", value=dungeon.get('playersalive'), label_dict=labels)
            m.add_def("rf_dungeon_enemies_total", "Total number of enemies in the dungeon", value=dungeon.get('enemies'), label_dict=labels)
            m.add_def("rf_dungeon_enemies_alive_total", "Number of alive enemies", value=dungeon.get('enemiesalive'), label_dict=labels)
            m.add_def("rf_dungeon_duration_seconds", "Seconds since dungeon start", value=dungeon.get('secondsuntilstart'), label_dict=labels)
            m.add_def("rf_dungeons_total", "Number of dungeons since game start", MetricType.COUNTER, value=dungeon.get('count'), label_dict=labels)
            m.add_def("rf_dungeon_boss_health", "Current HP value of the dungeon boss", value=dungeon.get('boss', {}).get('health'), label_dict=labels)
            m.add_def("rf_dungeon_boss_combat_level", "Combat level of the dungeon boss", value=dungeon.get('boss', {}).get('combatlevel'), label_dict=labels)
            if dungeon and 'enemies' in dungeon:
                if not dungeon.get('started'):
                    dungeon_healths[session_.get("twitchusername")] = dungeon.get('boss', {}).get('health')
                else:
                    if dungeon.get('enemiesalive') > 0 or not session_.get("twitchusername") in dungeon_healths:
                        dungeon_healths[session_.get("twitchusername")] = dungeon.get('boss', {}).get('health')
            m.add_def("rf_dungeon_boss_max_health", "Maximum health of the dungeon boss", value=dungeon_healths.get(session_.get("twitchusername")), label_dict=labels)
        
        if validate_data(multiplier, multiplier_adapter, "GameMultiplier"):
            m.add_def("rf_multiplier_info", "Textual info about the multiplier", value=1, event_name=multiplier.get('eventname'), label_dict=labels)
            m.add_def("rf_multiplier_active", "Multiplier is active", value=multiplier.get('active'), label_dict=labels)
            m.add_def("rf_multiplier_elapsed_time_seconds", "Seconds since multiplier event started", value=multiplier.get('elapsed'), label_dict=labels)
            m.add_def("rf_multiplier_duration_seconds", "Duration of multiplier in seconds", value=multiplier.get('duration'), label_dict=labels)
            mult_start_ts = None
            mult_end_ts = None
            if "starttime" in multiplier:
                mult_start_ts = to_timestamp(multiplier['starttime'])
            if "endtime" in multiplier:
                mult_end_ts = to_timestamp(multiplier["endtime"])
            m.add_def("rf_multiplier_start_timestamp_seconds", "Start timestamp of multiplier", value=mult_start_ts, label_dict=labels)
            m.add_def("rf_multiplier_end_timestamp_seconds", "End timestamp of multiplier", value=mult_end_ts, label_dict=labels)
            m.add_def("rf_multiplier_value", "Current multiplier factor", value=multiplier.get('multiplier'), label_dict=labels)
        
        if validate_data(raid, raid_adapter, "Raid"):
            m.add_def("rf_raid_started", "Raid has started", value=raid.get('started'), label_dict=labels)
            m.add_def("rf_raid_players_total", "Number of players participating", value=raid.get('players'), label_dict=labels)
            m.add_def("rf_raid_time_remaining_seconds", "Time left until raid fails", value=raid.get('timeleft'), label_dict=labels)
            m.add_def("rf_raid_total", "Number of raids since game start", MetricType.COUNTER, value=raid.get('count'), label_dict=labels)
            m.add_def("rf_raid_boss_health", "Current health of the raid boss", value=raid.get('boss', {}).get('health'), label_dict=labels)
            m.add_def("rf_raid_boss_max_health", "Maximum health of the raid boss", value=raid.get('boss', {}).get('maxhealth'), label_dict=labels)
            m.add_def("rf_raid_boss_combat_level", "Combat level of the raid boss", value=raid.get('boss', {}).get('combatlevel'), label_dict=labels)
        
        if validate_data(ferry, ferry_adapter, "Ferry"):
            m.add_def(
                "rf_ferry_info", "Textual information about the ferry", value=1, 
                destination=ferry.get('destination'), captain=ferry.get('captain', {}).get('name', ''), label_dict=labels
            )
            m.add_def("rf_ferry_players_total", "Number of players on the ferry", value=ferry.get('players'), label_dict=labels)
            m.add_def("rf_ferry_captain_sailing_skill_level", "Sailing level of the captain", value=ferry.get('captain', {}).get('sailinglevel'), label_dict=labels)
            m.add_def("rf_ferry_boost_remaining_time_seconds", "Remaining time of the ferry boost", value=ferry['boost']['remainingtime'], label_dict=labels)
            m.add_def("rf_ferry_boost_active", "Ferry boost is active", value=ferry['boost']['isactive'], label_dict=labels)
        
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
        total_experience_by_skill: dict[str, float] = {}
        total_experience_by_session = 0
        if isinstance(players, dict):
            players = [players]
        if validate_data(players, player_list_adapter, "Players"):
            for player in players:
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
                    label_dict=labels
                )
                
                if player.get('resting'):
                    resting_count += 1
                
                player_labels = {
                    "player_name": player.get("name"),
                    "player_id": player.get("id"),
                }

                m.add_value("rf_player_rested_time_seconds", player.get('restedtime'), **player_labels, label_dict=labels)
                m.add_value("rf_player_command_last_execution_time_seconds", player.get('commandidletime'), **player_labels, label_dict=labels)
                m.add_value("rf_player_coins_total", player.get('coins'), **player_labels, label_dict=labels)
                m.add_value("rf_player_combat_level", player.get('stats', {}).get('combatlevel'), **player_labels, label_dict=labels)

                for stat_name, stat_info in cast(ItemsView[str, PlayerStat], player.get('stats', {}).items()):
                    if stat_name == "combatlevel":
                        continue
                    p_labels: dict[str, str] = {}
                    p_labels.update(player_labels)
                    p_labels.update(labels)
                    m.add_value(
                        f"rf_player_stat_base_level", 
                        value=stat_info.get('level'), label_dict=p_labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_max_level", 
                        value=stat_info.get('maxlevel'), label_dict=p_labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_value", 
                        value=stat_info.get('currentvalue'), label_dict=p_labels, stat=stat_name
                    )
                    m.add_value(
                        f"rf_player_stat_experience_since_last_level_up", 
                        value=stat_info.get('experience'), label_dict=p_labels, stat=stat_name
                    )
                    cumulative_exp = stat_info.get('experience')
                    for x in range(1,stat_info.get('level', 0)+1):
                        cumulative_exp += experience_for_level(x)
                    m.add_value(
                        f"rf_player_stat_experience_total", 
                        value=cumulative_exp, label_dict=p_labels, stat=stat_name
                    )
                    if not stat_name in total_experience_by_skill:
                        total_experience_by_skill[stat_name] = 0
                    total_experience_by_skill[stat_name] += cumulative_exp
                    total_experience_by_session += cumulative_exp
        m.add_def("rf_session_resting_players_total", "Total number of players currently resting", MetricType.COUNTER, value=resting_count, label_dict=labels)
        m.add_def("rf_session_experience_total", "Sum of all experience of all players in the session", value=total_experience_by_session, label_dict=labels)
        m.add_def("rf_session_experience_by_skill_total", "Sum of all experience of all players in the session categorized by skill")
        for skill_name, total_exp in total_experience_by_skill.items():
            m.add_value(
                "rf_session_experience_by_skill_total", total_exp, stat=skill_name, label_dict=labels
            )
        
        if validate_data(village, village_adapter, "Village"):
            boost_stats: list[str] = []
            for a in village.get('boost', '').split(', '):
                split = a.split(' ', maxsplit=1)
                if len(split) == 2:
                    stat, percent = split
                else:
                    continue
                boost_stats.append(stat)
                m.add_def(
                    "rf_village_boost_percent", "Village boost percentage",
                    value=float(percent.strip("%")), label_dict=labels, stat=stat
                )
            boost_stats_text = ','.join(boost_stats)
            m.add_def("rf_village_info", "Textual info about the village", value=1, name=village.get('name'), boost=village.get('boost'), boost_stats=boost_stats_text, label_dict=labels)
            m.add_def("rf_village_tier_level", "Village tier level", value=village.get('tier'), label_dict=labels)
            m.add_def("rf_village_level", "Village level", value=village.get('level'), label_dict=labels)
    # t1 = time.monotonic()
    # print((t1-t0)*1000)
    # t0 = time.monotonic()
    text = m.get_text()
    # t1 = time.monotonic()
    # print((t1-t0)*1000)
    # yappi.get_func_stats().print_all()
    # yappi.get_thread_stats().print_all()
    return text

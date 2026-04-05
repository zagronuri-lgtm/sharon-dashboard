#!/usr/bin/env python3
"""
preprocess.py — Reads survey CSVs + shapefile, aggregates data, generates dashboard HTML.
"""

import pandas as pd
import geopandas as gpd
import json
import os
import glob
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENC = 'cp1255'

# --- Helpers ---
def read_csv_safe(filename, **kwargs):
    path = os.path.join(BASE_DIR, filename)
    for enc in [ENC, 'utf-8-sig', 'utf-8', 'latin-1']:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except (UnicodeDecodeError, LookupError):
            continue
    raise RuntimeError(f"Cannot read {filename}")

def find_obod():
    """Find the obod.csv file with invisible Unicode chars in name."""
    for f in os.listdir(BASE_DIR):
        if 'obod' in f.lower():
            return os.path.join(BASE_DIR, f)
    raise FileNotFoundError("obod.csv not found")

def to_float(s):
    try:
        return float(str(s).replace(',', ''))
    except:
        return 0.0

# --- Load Data ---
print("Loading trips.csv...")
trips = read_csv_safe('trips.csv')
sharon_clusters = ['שרון חולון מרחבי', 'השרון']
trips_sharon = trips[trips['cluster'].isin(sharon_clusters)].copy()

print("Loading obad.csv...")
obad = read_csv_safe('obad.csv')
# Filter obad to sharon trips
sharon_trip_ids = set(trips_sharon['trip_id_unique'].dropna().unique())
obad_sharon = obad[obad['trip_id_unique'].isin(sharon_trip_ids)].copy()

print("Loading obod.csv...")
obod_path = find_obod()
obod = pd.read_csv(obod_path, encoding=ENC)
# Filter obod to sharon trips
obod_sharon = obod[obod['trip_id_unique'].isin(sharon_trip_ids)].copy()

# --- 1. KPI Summary ---
print("Computing KPIs...")
total_riders = trips_sharon['total_up_menupach'].sum()
total_km = trips_sharon['total_km'].sum()
num_trips = len(trips_sharon)
num_lines = trips_sharon['line'].nunique()
avg_speed = (trips_sharon['speed'] * trips_sharon['total_km']).sum() / trips_sharon['total_km'].sum() if total_km > 0 else 0

# Captive riders from obod
vehicles = obod_sharon['vehicles_household'].dropna()
weight = obod_sharon.loc[vehicles.index, 'mekadem_nipuach_quests'].fillna(1)
captive_count = (vehicles.astype(str).str.strip().isin(['0', '0.0']) * weight).sum()
total_weighted = weight.sum()
captive_pct = round(captive_count / total_weighted * 100, 1) if total_weighted > 0 else 31.2

kpi_summary = {
    'total_riders': round(total_riders),
    'num_lines': int(num_lines),
    'total_km': round(total_km),
    'avg_speed': round(avg_speed, 1),
    'num_trips_sampled': int(num_trips),
    'captive_pct': captive_pct
}

# --- 2. Top Lines ---
print("Computing top lines...")
line_agg = trips_sharon.groupby(['line', 'cluster']).agg(
    riders=('total_up_menupach', 'sum'),
    km=('total_km', 'sum'),
    trips_sampled=('trip_id_unique', 'count'),
    total_speed_km=('speed', lambda x: (x * trips_sharon.loc[x.index, 'total_km']).sum()),
    total_km_for_speed=('total_km', 'sum')
).reset_index()
line_agg['avg_speed'] = (line_agg['total_speed_km'] / line_agg['total_km_for_speed']).round(1)
line_agg['efficiency'] = (line_agg['riders'] / line_agg['km']).round(2)
line_agg = line_agg.sort_values('riders', ascending=False).head(15)

top_lines = []
for _, r in line_agg.iterrows():
    top_lines.append({
        'line': int(r['line']) if pd.notna(r['line']) else 0,
        'cluster': r['cluster'],
        'riders': round(r['riders']),
        'km': round(r['km']),
        'trips_sampled': int(r['trips_sampled']),
        'avg_speed': float(r['avg_speed']),
        'efficiency': float(r['efficiency'])
    })

# --- 2b. ALL Lines lookup (for dynamic bot queries) ---
print("Building all-lines lookup...")
all_line_agg = trips_sharon.groupby('line').agg(
    riders=('total_up_menupach', 'sum'),
    km=('total_km', 'sum'),
    trips_sampled=('trip_id_unique', 'count'),
    line_info=('line_info', 'first')
).reset_index()
all_line_agg['avg_speed'] = trips_sharon.groupby('line').apply(
    lambda g: (g['speed'] * g['total_km']).sum() / g['total_km'].sum() if g['total_km'].sum() > 0 else 0
).values
all_line_agg['efficiency'] = (all_line_agg['riders'] / all_line_agg['km']).round(2)

all_lines_lookup = {}
for _, r in all_line_agg.iterrows():
    all_lines_lookup[str(int(r['line']))] = {
        'line': int(r['line']),
        'riders': round(r['riders']),
        'km': round(r['km']),
        'avg_speed': round(float(r['avg_speed']), 1),
        'efficiency': float(r['efficiency']),
        'route': str(r['line_info'])[:80] if pd.notna(r['line_info']) else ''
    }

# --- 3. Top Stations ---
print("Computing top stations...")
obad_sharon['passengers_up_menupach'] = pd.to_numeric(obad_sharon['passengers_up_menupach'], errors='coerce').fillna(0)
obad_sharon['passengers_down_menupach'] = pd.to_numeric(obad_sharon['passengers_down_menupach'], errors='coerce').fillna(0)
obad_sharon['latitude'] = pd.to_numeric(obad_sharon['latitude'], errors='coerce')
obad_sharon['longitude'] = pd.to_numeric(obad_sharon['longitude'], errors='coerce')

station_agg = obad_sharon.groupby(['station_id', 'station_name']).agg(
    boardings=('passengers_up_menupach', 'sum'),
    alightings=('passengers_down_menupach', 'sum'),
    lat=('latitude', 'first'),
    lon=('longitude', 'first')
).reset_index()
station_agg = station_agg.sort_values('boardings', ascending=False).head(30)

top_stations = []
for _, r in station_agg.iterrows():
    if pd.notna(r['lat']) and pd.notna(r['lon']):
        top_stations.append({
            'id': str(r['station_id']),
            'name': str(r['station_name']),
            'boardings': round(r['boardings']),
            'alightings': round(r['alightings']),
            'lat': round(float(r['lat']), 5),
            'lon': round(float(r['lon']), 5)
        })

# --- 4. Demographics ---
print("Computing demographics...")
def compute_distribution(col_name, weight_col='mekadem_nipuach_quests'):
    col = obod_sharon[col_name].dropna()
    col = col[col.astype(str).str.strip() != 'Null']
    w = obod_sharon.loc[col.index, weight_col].fillna(1).astype(float)
    df = pd.DataFrame({'label': col.astype(str).str.strip(), 'weight': w})
    grouped = df.groupby('label')['weight'].sum().sort_values(ascending=False)
    total = grouped.sum()
    return [{'label': k, 'value': round(v), 'percent': round(v/total*100, 1)} for k, v in grouped.items()]

demographics = {
    'age': compute_distribution('age'),
    'employment': compute_distribution('employment_status'),
    'vehicles': compute_distribution('vehicles_household'),
    'trip_frequency': compute_distribution('trip_frequency'),
    'access_mode': compute_distribution('transprt_from_orig'),
    'gender': compute_distribution('gender')
}

# --- 5. Time Distribution ---
print("Computing time distribution...")
time_agg = trips_sharon.groupby(['day_time_period', 'cluster']).agg(
    riders=('total_up_menupach', 'sum')
).reset_index()
total_by_cluster = time_agg.groupby('cluster')['riders'].sum()
time_data = []
for _, r in time_agg.iterrows():
    cl_total = total_by_cluster.get(r['cluster'], 1)
    time_data.append({
        'period': str(r['day_time_period']),
        'cluster': str(r['cluster']),
        'riders': round(r['riders']),
        'percent': round(r['riders'] / cl_total * 100, 1)
    })

# --- 6. OD Matrix ---
print("Computing OD matrix...")
obod_sharon['mekadem_nipuach_quests'] = pd.to_numeric(obod_sharon['mekadem_nipuach_quests'], errors='coerce').fillna(1)
od = obod_sharon.groupby(['SHEM_YISHU_orig', 'SHEM_YISHU_dest']).agg(
    trips=('mekadem_nipuach_quests', 'sum')
).reset_index()
od = od.sort_values('trips', ascending=False)
od_total = od['trips'].sum()

od_top10 = []
for _, r in od.head(15).iterrows():
    od_top10.append({
        'origin': str(r['SHEM_YISHU_orig']),
        'destination': str(r['SHEM_YISHU_dest']),
        'trips': round(r['trips']),
        'percent': round(r['trips'] / od_total * 100, 1)
    })

# Sharon-TA axis
sharon_cities = ['נתניה', 'הרצליה', 'רמת השרון', 'רעננה', 'כפר סבא', 'הוד השרון']
ta = 'תל אביב -יפו'
sharon_ta_axis = []
for city in sharon_cities:
    to_ta = od[(od['SHEM_YISHU_orig'] == city) & (od['SHEM_YISHU_dest'] == ta)]['trips'].sum()
    from_ta = od[(od['SHEM_YISHU_orig'] == ta) & (od['SHEM_YISHU_dest'] == city)]['trips'].sum()
    sharon_ta_axis.append({
        'city': city,
        'to_ta': round(to_ta),
        'from_ta': round(from_ta),
        'total': round(to_ta + from_ta)
    })
sharon_ta_axis.sort(key=lambda x: x['total'], reverse=True)

# Internal Sharon OD
internal = od[od['SHEM_YISHU_orig'].isin(sharon_cities) & od['SHEM_YISHU_dest'].isin(sharon_cities)]
internal = internal[internal['SHEM_YISHU_orig'] != internal['SHEM_YISHU_dest']]
internal_top = []
for _, r in internal.head(10).iterrows():
    internal_top.append({
        'origin': str(r['SHEM_YISHU_orig']),
        'destination': str(r['SHEM_YISHU_dest']),
        'trips': round(r['trips'])
    })

od_matrix = {
    'top_overall': od_top10,
    'sharon_ta_axis': sharon_ta_axis,
    'internal_sharon': internal_top
}

# --- 7. Station GeoJSON ---
print("Building station GeoJSON...")
station_geo_data = obad_sharon.groupby('station_id').agg(
    name=('station_name', 'first'),
    boardings=('passengers_up_menupach', 'sum'),
    alightings=('passengers_down_menupach', 'sum'),
    lat=('latitude', 'first'),
    lon=('longitude', 'first')
).reset_index()
station_geo_data = station_geo_data.dropna(subset=['lat', 'lon'])
station_geo_data = station_geo_data[station_geo_data['boardings'] > 10]  # filter noise

station_geo = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(float(r['lon']), 5), round(float(r['lat']), 5)]},
            "properties": {
                "name": str(r['name']),
                "boardings": round(r['boardings']),
                "alightings": round(r['alightings']),
                "id": str(r['station_id'])
            }
        }
        for _, r in station_geo_data.iterrows()
    ]
}

# --- 8. Boundaries GeoJSON ---
print("Loading shapefile...")
try:
    shp_path = os.path.join(BASE_DIR, 'statistical_areas_2022.shp')
    gdf = gpd.read_file(shp_path)
    # Filter to Sharon municipalities
    sharon_names = ['נתניה', 'הרצליה', 'רמת השרון', 'רעננה', 'כפר סבא', 'הוד השרון', 'כפר יונה',
                    'טייבה', 'טירה', 'קלנסווה', 'תל אביב -יפו', 'חולון', 'בת ים', 'פתח תקווה']
    gdf_sharon = gdf[gdf['SHEM_YISHU'].isin(sharon_names)].copy()
    # Reproject from ITM to WGS84
    if gdf_sharon.crs and gdf_sharon.crs.to_epsg() != 4326:
        gdf_sharon = gdf_sharon.to_crs(epsg=4326)
    # Simplify
    gdf_sharon['geometry'] = gdf_sharon['geometry'].simplify(0.001)
    # Dissolve to city level for smaller output
    gdf_cities = gdf_sharon.dissolve(by='SHEM_YISHU', as_index=False)[['SHEM_YISHU', 'geometry']]
    boundaries_geo = json.loads(gdf_cities.to_json())
except Exception as e:
    print(f"Warning: Could not process shapefile: {e}")
    boundaries_geo = {"type": "FeatureCollection", "features": []}

# --- 9. Speed Analysis (from analysis document) ---
speed_analysis = [
    {"line": 126, "avg_speed": 14.1, "threshold": 18, "gap": -3.9, "route": "חולון-ת\"א, ציר עמוס"},
    {"line": 24, "avg_speed": 16.6, "threshold": 18, "gap": -1.4, "route": "רמת השרון-ת\"א"},
    {"line": 501, "avg_speed": 18.5, "threshold": 18, "gap": 0.5, "route": "רעננה-ת\"א — גבולי"},
    {"line": 47, "avg_speed": 19.9, "threshold": 18, "gap": 1.9, "route": "תקין"},
    {"line": 48, "avg_speed": 19.6, "threshold": 18, "gap": 1.6, "route": "תקין"},
    {"line": 561, "avg_speed": 20.8, "threshold": 18, "gap": 2.8, "route": "תקין"}
]

# --- 10. Rail Competition (hardcoded from analysis) ---
rail_competition = [
    {"line": 615, "riders_per_km": 0.12, "riders": 287, "km": 2392, "od": "ת\"א ↔ כפר שמריהו", "action": "ביטול מיידי", "level": "extreme"},
    {"line": 616, "riders_per_km": 0.12, "riders": 301, "km": 2508, "od": "ת\"א ↔ הרצליה ת.רכבת", "action": "ביטול מיידי", "level": "extreme"},
    {"line": 617, "riders_per_km": 0.57, "riders": 826, "km": 1449, "od": "ת.רכבת הרצליה ↔ ת.רכבת נתניה", "action": "ביטול מיידי", "level": "extreme"},
    {"line": 623, "riders_per_km": 0.34, "riders": 543, "km": 1597, "od": "ת\"א ↔ מרכז נתניה", "action": "ביטול", "level": "high"},
    {"line": 610, "riders_per_km": 0.43, "riders": 763, "km": 1774, "od": "ת\"א ↔ ת.רכבת נתניה", "action": "ביטול", "level": "high"},
    {"line": 611, "riders_per_km": 0.48, "riders": 654, "km": 1363, "od": "ת\"א ↔ הרצליה פיתוח", "action": "ביטול", "level": "high"},
    {"line": 619, "riders_per_km": 0.47, "riders": 734, "km": 1561, "od": "רמת השרון ↔ נתניה", "action": "קיצור", "level": "medium"},
    {"line": 608, "riders_per_km": 0.78, "riders": 1543, "km": 1978, "od": "ת\"א ↔ ת.רכבת נתניה", "action": "ביטול", "level": "high"},
    {"line": 609, "riders_per_km": 0.59, "riders": 978, "km": 1658, "od": "ת\"א ↔ כפר ויתקין", "action": "ביטול", "level": "medium"},
    {"line": 650, "riders_per_km": 0.65, "riders": 1112, "km": 1711, "od": "הרצליה ↔ נתניה", "action": "קיצור + הפחתה", "level": "medium"},
    {"line": 605, "riders_per_km": 1.43, "riders": 2284, "km": 1597, "od": "מרכז ת\"א ↔ מרכז נתניה", "action": "שמירה + שיפור", "level": "low"},
    {"line": 600, "riders_per_km": 1.55, "riders": 1843, "km": 1189, "od": "ת\"א ↔ הרצליה/רעננה", "action": "שמירה", "level": "low"},
    {"line": 601, "riders_per_km": 2.15, "riders": 3403, "km": 1583, "od": "ת.רכבת נתניה ↔ מרכז נתניה", "action": "חיזוק", "level": "complement"}
]

# --- 11. Proposed Lines ---
proposed_lines = [
    {
        "id": "A", "name": "השרון המהיר",
        "route": "ת\"מ הרצליה → תע\"ש → כביש 4 → מסוף רעננה → ויצמן רעננה → מרכז כפר סבא",
        "length_km": 18, "travel_time": "30 דקות", "frequency": "10 דקות 06:00-21:00",
        "estimated_riders": "2,500-3,500", "annual_cost": "₪4-5M",
        "justification": "ציר #1 פנים-שרוני, מחבר מוקדי תעסוקה",
        "color": "#3b82f6",
        "coords": [[34.7912, 32.1629], [34.8100, 32.1650], [34.8300, 32.1800], [34.8459, 32.1913], [34.8550, 32.1950], [34.8700, 32.1850]]
    },
    {
        "id": "B", "name": "הוד-רבין",
        "route": "הוד השרון מרכז → כביש 5 → ת. רכבת הוד השרון → ת\"מ רבין → מרכז פ\"ת",
        "length_km": 22, "travel_time": "35-40 דקות", "frequency": "15 דקות 06:00-20:00",
        "estimated_riders": "800-1,500", "annual_cost": "₪3-4M",
        "justification": "כיסוי פנים-שרוני חסר, ביקוש בריאות",
        "color": "#10b981",
        "coords": [[34.8880, 32.1530], [34.8900, 32.1400], [34.8750, 32.1200], [34.8700, 32.1000], [34.8830, 32.0900]]
    },
    {
        "id": "C", "name": "מזין M1 — כפר סבא צפון",
        "route": "כפר סבא צפון → תחנת מטרו כפר סבא מזרח",
        "length_km": 7, "travel_time": "15 דקות", "frequency": "8 דקות שיא, 15 רגיל",
        "estimated_riders": "1,200-2,000", "annual_cost": "₪2M",
        "justification": "הכנה לפתיחת מטרו M1",
        "color": "#f59e0b",
        "coords": [[34.9000, 32.1950], [34.8950, 32.1880], [34.8850, 32.1800], [34.8700, 32.1850]]
    },
    {
        "id": "D", "name": "נתניה דרום",
        "route": "נתניה דרום → ת\"ח נתניה → כביש 2 → קיסריה תעסוקה",
        "length_km": 20, "travel_time": "35 דקות", "frequency": "20 דקות",
        "estimated_riders": "600-1,000", "annual_cost": "₪2-3M",
        "justification": "גידול אוכלוסייה + מרכז תעסוקה קיסריה",
        "color": "#8b5cf6",
        "coords": [[34.8500, 32.3000], [34.8600, 32.3200], [34.8700, 32.3500], [34.8900, 32.4000]]
    }
]

# --- 12. Service Gaps ---
gaps = [
    {"id": 1, "title": "ציר הרצליה-רעננה", "demand": "2,095 נסיעות/יום", "status": "שירות חלש — קו 29 מוגבל",
     "recommendation": "קו חדש / שדרוג קו 29 + BRT על כביש 4", "priority": "קריטי", "priority_color": "#ef4444"},
    {"id": 2, "title": "ציר נתניה-הרצליה", "demand": "2,063 נסיעות/יום", "status": "קווים 605/601 — איטיים ודלילים",
     "recommendation": "הגדלת תדירות + תחנות ביניים", "priority": "גבוה", "priority_color": "#f59e0b"},
    {"id": 3, "title": "הוד השרון-פתח תקווה", "demand": "1,992 נסיעות לת\"א (דרך כ\"ס)", "status": "ללא חיבור ישיר לרבין",
     "recommendation": "קו חדש הוד השרון-רבין-פ\"ת", "priority": "גבוה", "priority_color": "#f59e0b"},
    {"id": 4, "title": "שעות ערב (19:00-23:00)", "demand": "10.8% מהנסיעות = ~12,000/יום", "status": "רוב הקווים מצמצמים אחרי 21:00",
     "recommendation": "תדירות 20 דקות עד 23:00 בצירים ראשיים", "priority": "בינוני", "priority_color": "#3b82f6"},
    {"id": 5, "title": "ישובים ערביים (טייבה, טירה, קלנסווה)", "demand": "~150,000 תושבים — חסר נתונים בסקר", "status": "שירות בינעירוני מוגבל",
     "recommendation": "סקר ייעודי + קו ניסיוני לרעננה/כ\"ס", "priority": "בינוני", "priority_color": "#3b82f6"},
    {"id": 6, "title": "סנכרון רכבת-אוטובוס", "demand": "13.5% מהנוסעים מגיעים בהעברה", "status": "לוחות לא מסונכרנים",
     "recommendation": "סנכרון לוחות זמנים + פידרים לתחנות", "priority": "גבוה", "priority_color": "#f59e0b"}
]

# --- 13. Implementation & KPIs ---
implementation = {
    "phases": [
        {"id": "א", "timeframe": "0-6 חודשים", "label": "מיידי",
         "actions": ["Transit Signal Priority בצירים 24/501/532", "הגדלת תדירות PM peak בקווים 126, 501, 24", "סנכרון לוחות זמנים לרכבת"],
         "cost": "₪3.5-7M", "color": "#ef4444"},
        {"id": "ב", "timeframe": "6-18 חודשים", "label": "קצר",
         "actions": ["שדרוג קו 29 (הרצליה-רעננה-כ\"ס)", "קו חדש A: השרון המהיר", "שיפור תחנות מרכזיות"],
         "cost": "₪9-14M", "color": "#f59e0b"},
        {"id": "ג", "timeframe": "18-36 חודשים", "label": "בינוני",
         "actions": ["קו חדש B: הוד-רבין", "סקר ישובים ערביים + קו ניסיוני", "פידרי M1 (כ\"ס, רעננה, ר\"ה)"],
         "cost": "₪5-10M", "color": "#3b82f6"},
        {"id": "ד", "timeframe": "36-120 חודשים", "label": "ארוך",
         "actions": ["עיצוב מחדש כולל של הרשת לקראת M1", "BRT על כביש 4", "רשת פידרים מלאה"],
         "cost": "₪50M+", "color": "#8b5cf6"}
    ],
    "kpis": [
        {"metric": "מהירות קו 126", "baseline": "14.1 קמ\"ש", "target_12m": "17 קמ\"ש", "target_36m": "20 קמ\"ש"},
        {"metric": "מהירות קו 501", "baseline": "18.5 קמ\"ש", "target_12m": "20 קמ\"ש", "target_36m": "22 קמ\"ש"},
        {"metric": "עולים/יום (שרון)", "baseline": "~80,000", "target_12m": "90,000", "target_36m": "110,000"},
        {"metric": "כיסוי PM Peak", "baseline": "—", "target_12m": "85%", "target_36m": "95%"},
        {"metric": "תדירות הרצליה-רעננה", "baseline": "15-20 דקות", "target_12m": "12 דקות", "target_36m": "8 דקות"},
        {"metric": "כיסוי ישובים ערביים", "baseline": "נמוך", "target_12m": "ניסיוני", "target_36m": "מלא"}
    ]
}

# --- 14. Trade-off ---
tradeoff = {
    "cancel": [
        {"line": 616, "km_freed": 2508, "riders_affected": 301, "alternative": "רכבת ישירה"},
        {"line": 615, "km_freed": 2392, "riders_affected": 287, "alternative": "רכבת + קו 605"},
        {"line": 617, "km_freed": 1449, "riders_affected": 826, "alternative": "רכבת ישירה"},
        {"line": 610, "km_freed": 1774, "riders_affected": 763, "alternative": "רכבת + הסעה"},
        {"line": 623, "km_freed": 1597, "riders_affected": 543, "alternative": "רכבת + 601/605"},
        {"line": 611, "km_freed": 1363, "riders_affected": 654, "alternative": "קו 605 + רכבת"},
        {"line": 608, "km_freed": 1978, "riders_affected": 1543, "alternative": "קו 601 + רכבת"},
        {"line": 619, "km_freed": 780, "riders_affected": 280, "alternative": "רכבת החוף"},
        {"line": 650, "km_freed": 600, "riders_affected": 350, "alternative": "רכבת + 601"}
    ],
    "invest": [
        {"project": "קו 126 — הגדלת תדירות + BRT", "km_allocated": 2500, "expected_riders": "+25% נוסעים"},
        {"project": "קו 501 — הגברת תדירות PM", "km_allocated": 1800, "expected_riders": "+3,000/יום"},
        {"project": "קו 601 — הגדלת תדירות", "km_allocated": 1200, "expected_riders": "+1,500/יום"},
        {"project": "קו 571 — שיפור מסלול", "km_allocated": 800, "expected_riders": "+30% מהירות"},
        {"project": "קו חדש הרצליה-רעננה", "km_allocated": 3000, "expected_riders": "2,000/יום"},
        {"project": "קו חדש ר\"ה-ת\"א מהיר", "km_allocated": 2500, "expected_riders": "1,500/יום"}
    ],
    "summary": {
        "km_freed": 14441, "km_invested": 11800, "km_saved": 2641,
        "riders_affected": 5547, "new_riders": 8000,
        "annual_savings": "₪1.3-2.6M", "roi": "2.5x"
    }
}

# --- 15. Q&A Knowledge Base ---
qa_knowledge = [
    {"id": 1, "q": "מהו הקו העמוס ביותר?", "keywords": ["קו", "עמוס", "מוביל", "126", "נוסעים", "ראשון"],
     "a": "קו 126 (חולון-ת\"א) הוא הקו העמוס ביותר עם 9,639 עולים מנופח, יעילות 7.13 נוסעים/ק\"מ. אחריו קו 501 (רעננה-ת\"א) עם 9,508 עולים.", "cat": "ridership"},
    {"id": 2, "q": "כמה נוסעים יש בשרון?", "keywords": ["כמה", "נוסעים", "סהכ", "שרון", "עולים", "סך"],
     "a": "סך הנוסעים המנופח באשכולות השרון: ~113,000 עולים (92,000 באשכול 'שרון חולון מרחבי' + 21,000 באשכול 'השרון'). מתוך 1,937 נסיעות שנדגמו.", "cat": "ridership"},
    {"id": 3, "q": "מהי התחנה העמוסה ביותר?", "keywords": ["תחנה", "עמוסה", "עולים", "מרכזית", "מובילה"],
     "a": "ת. מרכזית תל אביב (קומה 6) — 3,056 עולים. אחריה ת. רכבת סבידור/נמיר (2,511 עולים) וקניון עזריאלי/בגין (2,138 עולים).", "cat": "stations"},
    {"id": 4, "q": "מה פרופיל הגיל של הנוסעים?", "keywords": ["גיל", "צעירים", "מבוגרים", "פרופיל", "דמוגרפיה"],
     "a": "25-44: 36.7% (עובדים), 19-24: 20.9% (סטודנטים), 45-64: 17%, 15-18: 11.3% (תלמידים), 65+: 9.6% (פנסיונרים), 8-14: 3.2%.", "cat": "demographics"},
    {"id": 5, "q": "כמה נוסעים בעלי רכב?", "keywords": ["רכב", "בעלות", "שבויים", "captive", "רכבים", "אוטו"],
     "a": "31.2% חסרי רכב (captive riders), 38.4% בעלי רכב אחד, 17.5% בעלי 2+ רכבים. כשני שלישים מהנוסעים הם בעלי רכב שבחרו בתח\"צ — רגישים לאיכות השירות.", "cat": "demographics"},
    {"id": 6, "q": "מתי שעת השיא?", "keywords": ["שיא", "שעה", "עומס", "בוקר", "ערב", "צהריים", "peak"],
     "a": "שיא אחה\"צ (15:00-19:00) = 28.7% — גדול מהשיא בבוקר (06:30-08:30 = 17.5%). ממצא מפתיע: תת-שירות בשעות אחה\"צ.", "cat": "time"},
    {"id": 7, "q": "מהן הזרימות הגדולות ביותר?", "keywords": ["OD", "זרימות", "מוצא", "יעד", "ציר", "נסיעות"],
     "a": "ציר ת\"א-שרון: נתניה↔ת\"א (7,467 נסיעות), הרצליה↔ת\"א (6,830), ר. השרון↔ת\"א (5,538). פנים-שרוני: הרצליה↔רעננה (2,095), הרצליה↔נתניה (2,063).", "cat": "od"},
    {"id": 8, "q": "מה הבעיה עם מהירות?", "keywords": ["מהירות", "איטי", "פקק", "126", "בעיה"],
     "a": "קו 126: 14.1 קמ\"ש (תקן: 18) — פער קריטי של 3.9 קמ\"ש. קו 24: 16.6 קמ\"ש. קו 501: 18.5 קמ\"ש (גבולי). נדרש נתיב עדיפות לאוטובוס.", "cat": "speed"},
    {"id": 9, "q": "למה צריך לבטל קווים?", "keywords": ["ביטול", "בטל", "רכבת", "תחרות", "מקבילים", "כפולים"],
     "a": "9 קווי אוטובוס מקבילים לרכבת עם פחות מנוסע 1/ק\"מ. ביטולם משחרר ~14,441 ק\"מ = ₪1.3-2.6M/שנה. הנוסעים יעברו לרכבת + קווים משופרים. ROI של 2.5x.", "cat": "rail"},
    {"id": 10, "q": "מה הפער הכי חשוב?", "keywords": ["פער", "חשוב", "חסר", "שירות"],
     "a": "ציר הרצליה-רעננה: 2,095 נסיעות/יום, #2 פנים-שרוני, ללא קו ישיר מהיר. המלצה: קו חדש 'השרון המהיר' בתדירות 10 דקות.", "cat": "gaps"},
    {"id": 11, "q": "אילו קווים חדשים מוצעים?", "keywords": ["קו", "חדש", "מוצע", "הצעה", "חדשים"],
     "a": "4 קווים: (A) 'השרון המהיר' הרצליה-רעננה-כ\"ס, (B) 'הוד-רבין' הוד השרון-פ\"ת, (C) 'מזין M1' כ\"ס צפון, (D) 'נתניה דרום' לקיסריה.", "cat": "recommendations"},
    {"id": 12, "q": "מה התקציב הנדרש?", "keywords": ["תקציב", "עלות", "כסף", "מיליון", "השקעה"],
     "a": "שלב א (0-6 חודשים): ₪3.5-7M. שלב ב (6-18 חודשים): ₪9-14M. שלב ג (18-36 חודשים): ₪5-10M. שלב ד (36-120 חודשים): ₪50M+ (שילוב מטרו M1).", "cat": "recommendations"},
    {"id": 13, "q": "מתי ייפתח המטרו?", "keywords": ["מטרו", "M1", "M3", "עתידי", "תחנות"],
     "a": "מטרו M1 (76-85 ק\"מ, 59-62 תחנות) צפוי ב-2030-2035. תחנות בשרון: כפר סבא (5), רעננה (4), הרצליה, רמת השרון (4). נדרש תכנון רשת פידרים כבר עכשיו.", "cat": "recommendations"},
    {"id": 14, "q": "איך הנוסעים מגיעים לתחנה?", "keywords": ["הגעה", "תחנה", "רגל", "העברה", "אופניים", "גישה"],
     "a": "79.8% ברגל, 13.5% באוטובוס (העברה), 2.7% רכב כנוסע, 1.6% רכבת, 0.6% רכב כנהג, 0.3%+ אופניים/קורקינט. רוב הנוסעים מגיעים ברגל — מיקום תחנות קריטי.", "cat": "demographics"},
    {"id": 15, "q": "מי הנוסע הטיפוסי?", "keywords": ["טיפוסי", "נוסע", "מי", "פרופיל"],
     "a": "עובד/ת שכיר/ה (55.3%), בגיל 25-44 (36.7%), מגיע/ה ברגל (79.8%), נוסע/ת כל יום (42.1%), בעל/ת רכב אחד (38.4%), מגיע/ה משרון חולון (77%).", "cat": "demographics"},
    {"id": 16, "q": "מה קורה עם ישובים ערביים?", "keywords": ["ערבי", "טייבה", "טירה", "קלנסווה", "ערבים"],
     "a": "~150,000 תושבים (טייבה, טירה, קלנסווה) עם ייצוג חסר בסקר. שירות בינעירוני מוגבל. המלצה: סקר ייעודי + קו ניסיוני לרעננה/כ\"ס.", "cat": "gaps"},
    {"id": 17, "q": "מה לגבי שעות הערב?", "keywords": ["ערב", "לילה", "21", "23", "שעות"],
     "a": "10.8% מהנסיעות בשעות ערב (19:00+) = ~12,000/יום. רוב הקווים מצמצמים אחרי 21:00. המלצה: שמירה על תדירות 20 דקות עד 23:00.", "cat": "gaps"},
    {"id": 18, "q": "מה המתודולוגיה?", "keywords": ["מתודולוגיה", "שיטה", "walker", "ניתוח"],
     "a": "מבוססת על עקרונות Jarrett Walker (Human Transit): (1) Ridership vs Coverage, (2) Frequency is Freedom, (3) Network Coherence. מקורות: סקר OB נתיבי איילון + data.gov.il.", "cat": "general"},
    {"id": 19, "q": "מתי נעשה הסקר?", "keywords": ["סקר", "מתי", "תאריך", "תקופה", "קורונה"],
     "a": "ינואר 2020 - יוני 2022. תקופת קורונה — ינואר-פברואר 2020 (לפני קורונה) מהווים 51% מהנסיעות ומשמשים כבסיס אמין. ממצאים מבניים (OD, תחנות, פרופיל) עדיין רלוונטיים.", "cat": "general"},
    {"id": 20, "q": "מה ROI של השינויים?", "keywords": ["ROI", "תשואה", "חיסכון", "כדאיות"],
     "a": "כל ₪1 שנחסך מביטול קווים מקבילים → ₪2.5 ערך תחבורתי בקווים עמוסים. ביטול 9 קווים = ₪1.3-2.6M/שנה. השקעה מחדש = +8,000 נוסעים/יום.", "cat": "rail"},
    {"id": 21, "q": "כמה קווים פעילים בשרון?", "keywords": ["קווים", "פעילים", "כמה", "רשת"],
     "a": "~851 קווים: 399 באשכול שרון חולון מרחבי, 276 באשכול השרון, 89+87 בצירים לחיפה/ירושלים. סך ק\"מ שבועי: ~2,861,420.", "cat": "ridership"},
    {"id": 22, "q": "מה קו 605?", "keywords": ["605", "בינעירוני", "נתניה"],
     "a": "קו 605: מרכז ת\"א ↔ מרכז נתניה. 1.43 נוסעים/ק\"מ — נראה נמוך, אך 46% מנתניה ו-32% מת\"א נוסעים בין מרכזי ערים (לא תחנות רכבת). הרכבת לא מחליפה אותו. המלצה: שמירה + שיפור.", "cat": "rail"},
    {"id": 23, "q": "כמה אוכלוסייה בשרון?", "keywords": ["אוכלוסייה", "תושבים", "כמה", "דמוגרפיה", "ערים"],
     "a": "~850,000-1,000,000: נתניה (~250K), כפר סבא (~105K), הרצליה (~100K), רעננה (~85K), רמת השרון (~60K), הוד השרון (~57K), כפר יונה (~25K), ערים ערביות (~150K).", "cat": "general"},
    {"id": 24, "q": "מה עם יעילות הקווים?", "keywords": ["יעילות", "נוסעים", "קילומטר", "ביצוע"],
     "a": "מצוין (>6 נוס/קמ): קו 2 (9.27), 126 (7.13), 24 (6.91), 501 (6.38). טוב (4-6): 561, 15, 48. בינוני (2-4): 26, 149. נמוך (<1.5): 605, 608, 650.", "cat": "speed"},
    {"id": 25, "q": "מה קורה עם קו 617?", "keywords": ["617", "כפילות", "רכבת"],
     "a": "קו 617: מתחיל בת. רכבת הרצליה (58.1%) ומסיים בת. רכבת נתניה (31.4%). כפילות מוחלטת — הרכבת עושה את אותו מסלול ב-18 דקות (במקום 45 באוטובוס). המלצה: ביטול מיידי.", "cat": "rail"},
    {"id": 26, "q": "מה עם דיור חדש?", "keywords": ["דיור", "יחידות", "בנייה", "התחדשות", "חדש"],
     "a": "32 מתחמים חדשים בשרון = ~15,456 יח\"ד (2026-2032). הרצליה: ~4,600, כ\"ס: ~490, רעננה: מאות. צמיחה של 20-40% בביקוש תח\"צ תוך 10 שנים.", "cat": "general"},
    {"id": 27, "q": "מה תדירות הנסיעות של הנוסעים?", "keywords": ["תדירות", "כמה", "פעמים", "שבוע", "יומי"],
     "a": "42.1% נוסעים כל יום, 16.4% כמעט כל יום (= ~58.5% יומיים), 17.3% 1-2 פעמים בשבוע, 15.5% לעיתים רחוקות. 15.5% ניתנים להמרה עם שיפור שירות.", "cat": "demographics"},
    {"id": 28, "q": "מה ההמלצות המיידיות?", "keywords": ["מיידי", "דחוף", "המלצה", "עכשיו", "ראשון"],
     "a": "שלב א (0-6 חודשים): (1) Transit Signal Priority בצירים 24/501/532, (2) הגדלת תדירות PM peak בקווים 126/501/24, (3) סנכרון לוחות זמנים לרכבת. עלות: ₪3.5-7M.", "cat": "recommendations"},
    {"id": 29, "q": "מה קו השרון המהיר?", "keywords": ["שרון", "מהיר", "אקספרס", "חדש", "הרצליה", "רעננה"],
     "a": "קו חדש A: הרצליה → תע\"ש → כביש 4 → מסוף רעננה → כפר סבא. 18 ק\"מ, 30 דקות, תדירות 10 דקות. 2,500-3,500 נוסעים/יום. עלות: ₪4-5M/שנה.", "cat": "recommendations"},
    {"id": 30, "q": "מה עם חיבורי רכבת?", "keywords": ["רכבת", "חיבור", "פידר", "מזין", "סנכרון"],
     "a": "13.5% מהנוסעים מגיעים בהעברה מאוטובוס. תחנות רכבת רלוונטיות: רעננה (A+B), הרצליה, נתניה, כ\"ס. הבעיה: לוחות לא מסונכרנים. המלצה: סנכרון + קווים מזינים.", "cat": "gaps"},
    {"id": 31, "q": "איזה קו הכי חזק בנתניה?", "keywords": ["נתניה", "קו", "חזק", "מוביל", "עמוס"],
     "a": "בנתניה הקו המוביל הוא 605 (מרכז נתניה↔מרכז ת\"א) עם 5,587 עולים מנופח. אחריו קו 601 (מזין לרכבת נתניה, 3,403 עולים) וקו 613 (1,677 עולים). קו 605 הוא בינעירוני עם יעילות 1.43 נוס/ק\"מ — מוצדק כי מחבר מרכזי ערים ולא תחנות רכבת.", "cat": "ridership"},
    {"id": 32, "q": "איזה קו הכי חזק ברעננה?", "keywords": ["רעננה", "קו", "חזק", "מוביל", "עמוס"],
     "a": "ברעננה הקו המוביל הוא 501 (רעננה↔ת\"א) עם 9,395 עולים מנופח ויעילות מצוינת (6.58 נוס/ק\"מ). אחריו קו 48 (5,838 עולים, 4.18 נוס/ק\"מ) וקו 47 (5,102 עולים). קו 501 הוא אחד הקווים היעילים ביותר בשרון.", "cat": "ridership"},
    {"id": 33, "q": "איזה קו הכי חזק בכפר סבא?", "keywords": ["כפר סבא", "קו", "חזק", "מוביל", "עמוס"],
     "a": "בכפר סבא הקו המוביל הוא 149 (5,304 עולים, 2.74 נוס/ק\"מ). אחריו קו 567 (5,136 עולים) וקו 561 — הקו היעיל ביותר בכ\"ס עם 4,669 עולים ו-5.44 נוס/ק\"מ.", "cat": "ridership"},
    {"id": 34, "q": "איזה קו הכי חזק בהרצליה?", "keywords": ["הרצליה", "קו", "חזק", "מוביל", "עמוס"],
     "a": "הקווים המובילים העוברים בהרצליה (לפי הסקר): קו 605 (5,587 עולים, נתניה↔ת\"א), קו 601 (3,403 עולים), קו 501 (2,501 עולים, רעננה↔ת\"א). הקו הפנימי המוביל הוא 551 (1,383 עולים, הרצליה↔פ\"ת). קו 29 (הרצליה↔רעננה↔כ\"ס) לא נדגם בסקר אך פעיל ברשת הנוכחית — ניתן לשאול עליו ב'קו 29'.", "cat": "ridership"},
    {"id": 35, "q": "איזה קו הכי חזק בהוד השרון?", "keywords": ["הוד השרון", "קו", "חזק", "מוביל", "עמוס"],
     "a": "הוד השרון נסמכת בעיקר על קווים שעוברים דרך כפר סבא. הציר העיקרי הוא הוד השרון↔ת\"א עם 1,992 נסיעות/יום, אך אין קו ישיר חזק. זהו פער #3 — ולכן מוצע קו חדש B 'הוד-רבין' לחיבור ישיר למרכז רפואי רבין.", "cat": "gaps"},
    {"id": 36, "q": "איזה קו הכי חזק ברמת השרון?", "keywords": ["רמת השרון", "קו", "חזק", "מוביל", "עמוס"],
     "a": "ברמת השרון הקו המוביל הוא 24 (רמת השרון↔ת\"א) עם 7,091 עולים מנופח ויעילות מצוינת (6.91 נוס/ק\"מ). אחריו קו 26 (5,705 עולים). קו 24 סובל ממהירות גבולית (16.6 קמ\"ש) ומומלץ Transit Signal Priority.", "cat": "ridership"},
    {"id": 37, "q": "איזה קו הכי חזק בחולון?", "keywords": ["חולון", "קו", "חזק", "מוביל", "עמוס"],
     "a": "בחולון הקו המוביל הוא 126 (חולון↔ת\"א) עם 9,639 עולים מנופח — העמוס ביותר בכל השרון. יעילות 7.13 נוס/ק\"מ אך מהירות קריטית: 14.1 קמ\"ש בלבד (תקן: 18). נדרש נתיב עדיפות לאוטובוס בדחיפות.", "cat": "ridership"}
]

# --- Assemble all data ---
DATA = {
    'kpi_summary': kpi_summary,
    'top_lines': top_lines,
    'all_lines': all_lines_lookup,
    'top_stations': top_stations,
    'demographics': demographics,
    'time_distribution': time_data,
    'od_matrix': od_matrix,
    'station_geo': station_geo,
    'boundaries_geo': boundaries_geo,
    'speed_analysis': speed_analysis,
    'rail_competition': rail_competition,
    'proposed_lines': proposed_lines,
    'gaps': gaps,
    'implementation': implementation,
    'tradeoff': tradeoff,
    'qa_knowledge': qa_knowledge
}

# --- Write JSON to check ---
data_json = json.dumps(DATA, ensure_ascii=False, indent=None)
print(f"Total JSON size: {len(data_json):,} bytes ({len(data_json)/1024:.0f} KB)")

# --- Write placeholder for HTML injection ---
output_path = os.path.join(BASE_DIR, 'dashboard', 'data.json')
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(DATA, f, ensure_ascii=False, indent=2)

print(f"Data written to {output_path}")

# --- Generate self-contained HTML ---
html_template_path = os.path.join(BASE_DIR, 'dashboard', 'sharon_dashboard.html')
with open(html_template_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Inject data as EMBEDDED_DATA before the main script
data_script = f'<script>const EMBEDDED_DATA = {json.dumps(DATA, ensure_ascii=False)};</script>'
html = html.replace('<script>\n// ==================== DATA ====================', data_script + '\n<script>\n// ==================== DATA ====================')

standalone_path = os.path.join(BASE_DIR, 'dashboard', 'index.html')
with open(standalone_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Standalone HTML written to {standalone_path} ({os.path.getsize(standalone_path)/1024:.0f} KB)")

print("Done!")

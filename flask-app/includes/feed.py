from includes.maths import line_of_sight_distance
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

google_prefix = "https://www.google.com/maps/place/"
google_suffix = ",12z/data=!4m4!3m3!8m2!3d52.2803!4d0.657!5m1!1e1"

# From the source code
# define ADV_TYPE_NONE         0
# define ADV_TYPE_CHAT         1
# define ADV_TYPE_REPEATER     2
# define ADV_TYPE_ROOM         3
# define ADV_TYPE_SENSOR       4
node_types = {0: "NONE", 1: "CHAT", 2: "REPEATER", 3: "ROOM", 4: "SENSOR"}


def google_maps_ref(coords):
    # No point in doing anything if the coords are 0
    if coords[0] * coords[1] == 0:
        return "N/A"
    lat = coords[0]
    long = coords[1]
    return f'<A HREF="{google_prefix}{lat}+{long}/@{lat},{long}{google_suffix}" TARGET="maps">{lat:.3f}°, {long:.3f}°</A>'


def parse_feed(feed: str, hours: int):
    """
    Convert JSON :
    - First dictionary -> home node
    - Remaining rows -> advertised nodes

    # Data we're interested in
    name (name / adv_name)
    role (1 = chat client,2 = repeater,3 = room server?)
    location (adv_lat, adv_lon)
    distance (calculated)
    hops (out_path_len)
    last_advert (in epoch)

    """
    rows = []

    # Set a cutoff for nodes not heard from in 48 hours
    cutoff_dt = datetime.now(ZoneInfo("Europe/London")) - timedelta(hours=hours)

    rows.append(
        ["Name", "Role", "Location", "Distance", "Hops", f"Last heard (<{hours}hrs)"]
    )
    for idx, line in enumerate(feed):
        if idx == 0:  # home node
            home = [line.get("adv_lat"), line.get("adv_lon"), 0]
            rows.append(
                [line.get("name"), "CHAT", google_maps_ref(home), "N/A", "0", "N/A"]
            )
            print(rows)
        else:
            last_advert_epoch = line.get("last_advert")
            if not last_advert_epoch:
                continue

            last_heard_dt = datetime.fromtimestamp(
                last_advert_epoch, tz=ZoneInfo("Europe/London")
            )
            if last_heard_dt < cutoff_dt:
                continue

            coords = [line.get("adv_lat"), line.get("adv_lon"), 0]
            rows.append(
                [
                    line.get("adv_name"),
                    node_types[line.get("type")],
                    google_maps_ref(coords),
                    line_of_sight_distance(home, coords),
                    str(line.get("out_path_len")),
                    last_heard_dt.strftime("%Y-%m-%d %H:%M:%S"),
                ]
            )
    if not rows:
        return [], []

    headers = rows[0]
    data = rows[1:]

    # Sort by 'Last Heard' (index 5) in reverse order, except for the home node (index 0)
    if len(data) > 1:
        # Keep home node at top, sort the rest
        data = [data[0]] + sorted(data[1:], key=lambda x: x[5], reverse=True)

    return headers, data

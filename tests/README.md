These are example .json to validate and normalise

happy.json – all fields sensible
missing_temp.json – omit main.temp
humidity_140.json – main.humidity = 140
negative_rain.json – rain.1h = -1.0
dup_a.json / dup_b.json – same (id, dt), different main.temp
lat_91.json – coord.lat = 91
vis_50000.json – visibility = 50000
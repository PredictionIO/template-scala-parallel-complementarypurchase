# Complimentary Purchase Engine Template

## Documentation

Please refer to http://docs.prediction.io/templates/complimentarypurchase/quickstart/

## Versions

### v0.1.0

- initial version (require PredictionIO >= 0.9.0)

## Development Notes

### import sample data

```
$ python data/import_eventserver.py --access_key <your_access_key>
```

### sample query

```
$ curl -H "Content-Type: application/json" \
-d '{
  "items" : ["s2i1"],
  "num" : 3 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```


```
curl -H "Content-Type: application/json" \
-d '{
  "items" : ["s2i1", "s2i3", "s1i2"],
  "num" : 3 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "items" : ["s1i2", "s1i1"],
  "num" : 4 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```


```
$ curl -H "Content-Type: application/json" \
-d '{
  "items" : ["x", "s1i1"],
  "num" : 4 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "items" : ["i1"],
  "num" : 3 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
curl -H "Content-Type: application/json" \
-d '{
  "items" : ["p1", "p2", "p3"],
  "num" : 3 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "items" : ["i2", "i3"],
  "num" : 4 }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

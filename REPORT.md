# Assignment #4: Report

**Name: Lauren Sharpe**
**Student ID: 801371099**
**Email: lsharp22@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash
python datagen.py 801371099
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py $part /opt/spark/work-dir/shared 801371099
docker exec -u 0 spark-master python3 -m pip install --no-cache-dir numpy==2.2.6
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py $part /opt/spark/work-dir/shared 801371099
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared 801371099
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part2 /opt/spark/work-dir/shared 801371099
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part3 /opt/spark/work-dir/shared 801371099
$truthDir = Join-Path $env:TEMP ("a4-truth-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $truthDir | Out-Null
python datagen.py 801371099 $truthDir (Join-Path $truthDir "truth.csv")

```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```
=== part1a: micro-batch summary ===
batch input rows            watermark state rows
    0         38                    -         26
    1         36  2026-10-27 07:57:51         30
    2         42  2026-10-27 07:58:47         33
    3         37  2026-10-27 07:59:49         33
    4         41  2026-10-27 08:00:55         23
    5         45  2026-10-27 08:01:47         40
    6         44  2026-10-27 08:02:54         46
    7         43  2026-10-27 08:03:38         46
    8         43  2026-10-27 08:04:53         48
    9         36  2026-10-27 08:05:53         25
   10         40  2026-10-27 08:06:47         44
   11         42  2026-10-27 08:07:58         48
   12         35  2026-10-27 08:08:57         48
   13         38  2026-10-27 08:09:43         49
   14         37  2026-10-27 08:10:38         25
   15         40  2026-10-27 08:11:56         38
   16         42  2026-10-27 08:12:49         48
   17         40  2026-10-27 08:13:54         50
   18         37  2026-10-27 08:14:55         50
   19         35  2026-10-27 08:15:59         25
   20         41  2026-10-27 08:16:54         38
   21         38  2026-10-27 08:17:56         46
   22         38  2026-10-27 08:18:51         49
   23         39  2026-10-27 08:19:48         49
   24         40  2026-10-27 08:20:52         24
   25         42  2026-10-27 08:21:56         39
   26         44  2026-10-27 08:22:49         46
   27         39  2026-10-27 08:23:45         48
   28         42  2026-10-27 08:24:45         48
   29         36  2026-10-27 08:25:58         24
   30          0  2026-10-27 08:26:42         24
```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?

The stream wrote five minute windows starting at 7:55, 8:00, 8:05, 8:10, 8:15, and 8:20. The 8:25 window was missing from the stream output because its end time was 8:30 and the final watermark only reached 08:26. The printed tables were capped at 100 rows so the later windows were cut off in the display.
For the 8:00-8:05 window in pickup zone 1, the stream counted 14 trips and the batch counted 16. The batch included late rides regardless of arriable order. The stream missed two rides because they arived after spark had already closed the window. The state row count drops every five batches becuase the watermark passses the end of another five minute window and Spark can remove that window's zone totals. 



## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

9 of zero_distance

```
"R00022","2026-10-27 07:59:40","11","0.0","12.8","17.86","zero_distance"
"R00101","2026-10-27 08:02:32","18","0.0","5.2","7.95","zero_distance"
"R00140","2026-10-27 08:03:31","11","0.0","8.4","15.97","zero_distance"
"R00162","2026-10-27 08:02:08","1","0.0","50.6","56.55","zero_distance"
"R00182","2026-10-27 08:03:47","4","0.0","5.9","7.7","zero_distance"
```

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?
All 9 had zero distance, and none had a fare over $150. Each alert only depended on the current ride, so Spark did not need to remember previous rides or use a watermark. If I wanted flag drivers with more than three alerts in ten minutes, I would group alerts by driver and a ten minute event time window. Spark would need to keep counts in state and use a watermakr to hadnle late arrivals and close old windows.



---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
baseline   RMSE   3.79   R2  0.844
full       RMSE   1.72   R2  0.968
tuned      RMSE   1.72   R2  0.968

```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?


Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?
For the tuned model, I used linear regression with the same features as the full model and tested regParam values of 0.01 and 0.1. I wanted to see if adding a small amount of regularization would improve predictions. With two values and three folds, cross-validation ran six fits and then refit the best setting on the full training split. The full and tuned models both had an RMSE of 1.72 and an R² of 0.968 at the precision shown. Adding duration, zone, payment type, and hour improved the baseline, while tuning made little difference in the reported results. I turned hour into a string so the pipeline would treat each hour as a category. For example, the model can learn that 8 a.m. affects the fare differently from 9 a.m. without assuming fares go up by the same amount every hour.
The full linear pipeline was saved. It had a test RMSE of 1.72 and an R² of 0.968. The tuned model had the same scores after rounding.




---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 07:55:00|    2|
|2026-10-27 08:00:00|   11|
|2026-10-27 08:05:00|    8|
|2026-10-27 08:10:00|   12|
|2026-10-27 08:15:00|    9|
|2026-10-27 08:20:00|   12|
|2026-10-27 08:25:00|    4|
+-------------------+-----+
Part 3 scored 1,190 rides and flagged 58. The threshold was $6.88, which was four times the saved model’s reported test RMSE of $1.72. The largest deviation was for R00451. Its actual fare was $93.23, the predicted fare was $32.96, and the difference was $60.27.
```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?
I caught 38 of the 38 planted anomalies. Of the 58 flagged rides, 20 were not planted anomalies. My precision was 65.5% and my recall was 100%. Seven had a recorded distance of zero, including the three with the biggest errors. The other 13 were longer rides, ranging from about 13 to 26 km. I would handle zero-distance rides separately with the Part 1b rule. In Part 2, I would test whether letting distance and duration interact helps the model predict fares for longer rides. Then I would check whether that reduced false positives while still catching the planted anomalies.


Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

Part 3 uses transform() because the pipeline was already fitted in Part 2. Calling fit() on the streaming DataFrame would fail because these estimators need a bounded training dataset. The StringIndexerModel learned the category-to-index mappings for zone, payment type, and hour. The OneHotEncoderModel learned the category sizes used to build the encoded vectors. The LinearRegressionModel learned the coefficients and intercept for predicting fare. VectorAssembler just puts the feature columns together.


---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?

I would run the scorer as a Spark Structured Streaming job that reads rides from Kafka and sends flagged rides to a separate Kafka topic for the fraud team. A scheduled Spark batch job would retrain the model each night using the last month of completed rides. I would save each approved model and its threshold together in versioned storage and keep the streaming checkpoint in durable storage. To switch models, I would stop the scorer between batches and restart it with the new model and the same checkpoint so it resumes from its saved Kafka offsets. I would track processing lag, alert volume, prediction errors, and how many alerts the fraud team confirms. If errors rise or fewer alerts are confirmed over time, I would check whether the model still fits current rides.



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

pyspark.errors.exceptions.captured.IllegalArgumentException: [FIELD_NOT_FOUND] No such struct field `distance_kn` in `trip_id`, `driver_id`, `pickup_zone`, `dropoff_zone`, `pickup_ts`, `distance_km`, `duration_min`, `payment_type`, `fare`, `hour`, `zone`. SQLSTATE: 42704
  File "/opt/spark/work-dir/main.py", line 166, in part2_train
    paramGrid = ParamGridBuilder().addGrid(tuned_lr.regPram, [0.01,0.1]).build()
AttributeError: 'LinearRegression' object has no attribute 'regPram'. Did you mean: 'regParam'?

I had a few misspellings in part 2 that I fixed by making sure everything was spelled correctly. To prevent this in the future I should slow down when typing and check my code before running. 
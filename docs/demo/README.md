# HeatFlood Sentinel — Demonstration Video

<p align="center"><a href="https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/HeatFlood_Sentinel_Demo_1080p.mp4"><img src="thumb_1.png" alt="HeatFlood Sentinel demo video (4:59)" width="720"/></a></p>

A 4 min 59 s narrated walkthrough of the HeatFlood Sentinel dashboard and thesis results
(MS Data Science, NED University of Engineering and Technology — Aseef Ahmed; supervisor Dr. Maria Andaleeb Siddiqui).

**Release:** [demo-video-2026-10](https://github.com/SirAseef/heatflood-sentinel/releases/tag/demo-video-2026-10)

| File | Resolution | Size |
|---|---|---|
| [HeatFlood_Sentinel_Demo_4K.mp4](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/HeatFlood_Sentinel_Demo_4K.mp4) | 3840 × 2160, 30 fps | 176.5 MiB |
| [HeatFlood_Sentinel_Demo_1080p.mp4](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/HeatFlood_Sentinel_Demo_1080p.mp4) | 1920 × 1080, 30 fps | 73.3 MiB |
| [HeatFlood_Sentinel_Demo_720p.mp4](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/HeatFlood_Sentinel_Demo_720p.mp4) | 1280 × 720, 30 fps (small, for sharing) | 23.4 MiB |
| [HeatFlood_Sentinel_Demo.srt](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/HeatFlood_Sentinel_Demo.srt) | English subtitles | |
| [thumb_1.png](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/thumb_1.png) · [thumb_2.png](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/thumb_2.png) · [thumb_3.png](https://github.com/SirAseef/heatflood-sentinel/releases/download/demo-video-2026-10/thumb_3.png) | 1280 × 720 thumbnails | |

## What the video shows
Title card → Overview → Live Alerts → City pages (Karachi, Jacobabad) → Forecasting → Heatwave & flash-flood detection →
Model benchmarks → ConvLSTM skill figure → Explainability → Streaming & scalability → Scalability figure → Architecture →
Jacobabad 2022 case study and figure → Research / API / About → GitHub repository → closing card.

## How it was made and what the numbers mean
- Page clips were captured from the live site [siraseef.github.io/heatflood-sentinel](https://siraseef.github.io/heatflood-sentinel/)
  at 2× device scale (1920 × 1080 CSS px → 3840 px wide) on 26 Oct 2026; scrolling is composited from the captures, so CSS
  animations and hover states are not shown. The GitHub pages are static snapshots.
- Thesis results (forecasting, detection, benchmarks, case study) use ERA5 reanalysis 2015–2022 for 20 Pakistani cities,
  with 2022 held out as the test year.
- Streaming and scalability numbers shown on the dashboard page come from the local runtime (2 vCPU). The Big Data results
  narrated in the streaming segment (≈2,700 events/s Spark processing throughput on a 58,560-event backlog; kill-and-restart
  with 21,600/21,600 events in the Parquet sink, 0 missing, 0 duplicates) come from a **single-node pseudo-distributed**
  Hadoop 3.3.6 HDFS + YARN, Spark 3.5.9 and Kafka 3.9.2 (KRaft) cluster — see
  [`results/bigdata/RUN_LOG.md`](../../results/bigdata/RUN_LOG.md) and
  [`results/bigdata/streaming_kafka_spark.json`](../../results/bigdata/streaming_kafka_spark.json).
- Narration: Microsoft Edge neural TTS (en-US-AndrewNeural), loudness-normalised to −16 LUFS. No background music.

## Narration script
```
[00:00:00 – 00:00:18] s00_title · Title
HeatFlood Sentinel. A scalable big data and deep learning framework for heatwave and flash flood forecasting and detection in Pakistan. An M.S. Data Science thesis at NED University of Engineering and Technology, by Aseef Ahmed, supervised by Doctor Maria Andaleeb Siddiqui.

[00:00:18 – 00:00:49] s01_home · Overview
On-screen: 20 cities · ERA5 2015–2022 · 1,402,560 hourly rows · test year 2022 held out
Pakistan faces two linked climate hazards: extreme heat before the monsoon, and sudden flooding during it. HeatFlood Sentinel ingests eight years of ERA5 reanalysis for twenty Pakistani cities, one point four million hourly rows, and benchmarks statistical, machine learning and deep learning models on a fully held-out 2022 test year. The home page shows headline accuracy, the hazard map, and an alert replay from the streamed 2022 season.

[00:00:49 – 00:01:07] s02_live · Live Alerts
On-screen: Open-Meteo feed → streaming alert engine · refreshed every 6 h by GitHub Actions
The Live Alerts page runs the same models on real-time Open-Meteo data. Every six hours, a GitHub Actions workflow pulls fresh observations and forecasts, streams them through the alert engine, and republishes the map, the open alerts, and this all-cities table.

[00:01:07 – 00:01:24] s03_cities · City Pages
On-screen: One page per city: 72-hour outlook, 2022 forecasts vs ERA5, hazard probabilities
Each of the twenty cities has its own page. Here is Karachi, then Jacobabad: a seventy-two hour outlook, live hazard checks, daily forecasts against ERA5 for the 2022 test year, and the model's heatwave and flash flood probabilities through the season.

[00:01:24 – 00:01:40] s04_forecast · Forecasting
On-screen: Hourly LSTM, +1 h: RMSE 0.634 °C (persistence 1.558 °C)
For forecasting, the hourly LSTM predicts temperature one hour ahead with an error of just zero point six three four degrees, against one point five six for persistence. At six hours it still holds one point three seven degrees.

[00:01:40 – 00:02:10] s05_detect · Heatwave & Flash-Flood Detection
On-screen: Flash flood +6 h: XGBoost CSI 0.609 · Heatwave +1 d: persistence rule CSI 0.717 beats XGBoost 0.356
Detection is reported honestly. For flash floods six hours ahead, XGBoost reaches a critical success index of zero point six zero nine, ahead of the persistence rule. For heatwaves one day ahead, however, simple persistence still wins, with a CSI of zero point seven one seven against zero point three five six for XGBoost. Heat events last for days, so yesterday is a strong predictor, and the thesis says so plainly.

[00:02:10 – 00:02:32] s06_bench · Model Benchmarks
On-screen: ConvLSTM beats persistence at every lead, +1 h to +6 h (2° ERA5 grid)
The benchmark page compares LSTM, ConvLSTM, XGBoost, SARIMA, persistence and climatology, with Diebold-Mariano significance tests. On the gridded two-degree ERA5 lattice, the ConvLSTM beats persistence at every lead from one to six hours, for both temperature and rainfall.

[00:02:32 – 00:02:44] s07_fig_skill · Thesis figure
On-screen: Thesis figure · ConvLSTM vs persistence and climatology by lead
This thesis figure shows that gap widening with lead time: at six hours the ConvLSTM's temperature error is two point two nine degrees, against six point one seven for persistence.

[00:02:44 – 00:03:02] s08_explain · Explainability
On-screen: Top heatwave driver: Tmax anomaly, then p90 / p95 exceedance
Explainability shows why a model raised an alert. For the heatwave detector, the strongest driver is the daily maximum temperature anomaly, followed by exceedance of the ninetieth and ninety-fifth percentile thresholds. Saliency maps do the same for the ConvLSTM.

[00:03:02 – 00:03:43] s09_stream · Streaming & Scalability
On-screen: 87,840 events · 210.5 events/s · 0 lost / 0 duplicate alerts on kill-restart
The streaming pipeline replayed the full 2022 season: eighty-seven thousand eight hundred and forty events at two hundred and ten point five events per second. Under chaos tests, duplicates, corrupt records, late data and outages never crashed it, and on a kill and restart it lost zero alerts and produced zero duplicates. The same pipeline now also runs on a single-node Hadoop, YARN, Spark and Kafka cluster: about two thousand seven hundred events per second on a backlog, and a kill and restart test with zero lost and zero duplicate events in the output. The numbers on this page come from the local runtime.

[00:03:43 – 00:03:53] s10_fig_scal · Thesis figure
On-screen: Scalability · 2 workers: 1.88× speed-up, 94% efficiency (local, 2 vCPU)
Batch processing scales almost linearly with data volume, and two workers gave a one point eight eight times speed-up, at ninety-four percent efficiency.

[00:03:53 – 00:04:07] s11_arch · Architecture
On-screen: Bronze → Silver → Gold medallion lake · FastAPI · static export to GitHub Pages
The architecture follows a medallion data lake, from bronze raw data to silver cleaned data to gold features, feeding the models, the stream processor, and a FastAPI service that is exported to this static site.

[00:04:07 – 00:04:22] s12_case · Case Study: Jacobabad 2022
On-screen: Pre-monsoon heat (Apr–May) and monsoon floods, replayed on held-out data
The case study replays Jacobabad in 2022, one of the hottest cities on Earth, through its pre-monsoon heatwave and the monsoon floods that followed, showing exactly when the system would have raised each alert.

[00:04:22 – 00:04:25] s13_fig_case · Thesis figure
On-screen: Thesis figure · Jacobabad 2022 replay
Here is the same replay as it appears in the thesis.

[00:04:25 – 00:04:39] s14_rest · Research, API & About
On-screen: Thesis + 2 review manuscripts (in preparation) · OpenAPI docs · project details
Finally, the research page lists the thesis and two review manuscripts in preparation, the API docs describe every endpoint behind the dashboard, and the about page explains how the project is kept up to date.

[00:04:39 – 00:04:50] s15_github · Open Source
On-screen: github.com/SirAseef/heatflood-sentinel · release v0.1.0 · 58 tests · CI green
All of the code is open source on GitHub, released as version zero point one point zero, with fifty-eight automated tests passing in continuous integration.

[00:04:50 – 00:04:59] s16_close · Closing
Thank you for watching. Explore the live dashboard and the code at the links on screen.
```

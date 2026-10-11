# Grafana Dashboard Setup Guide

**Status**: ✅ Dashboards configured and auto-provisioned

---

## 🎯 Access Grafana

**URL**: http://localhost:3000  
**Username**: admin  
**Password**: admin

---

## 📊 Dashboards Available

### **PitWall Twin — F1 Telemetry Anomaly Detection**

**Auto-provisioned dashboard** with 6 panels:

1. **Speed Trace** — Speed over time for selected driver
2. **RPM Trace** — Engine RPM over time
3. **Residuals Heatmap** — 20 cars × time showing prediction errors
4. **Throttle Position** — Throttle input over time
5. **Brake Pressure** — Brake application over time
6. **Anomaly Detection Events** — Alert distribution

---

## 📈 How to Use

### **1. Navigate to Dashboard**

1. Open http://localhost:3000
2. Click **Dashboards** in left sidebar
3. Select **PitWall Twin — F1 Telemetry Anomaly Detection**

### **2. Select a Driver**

Dashboard has a **Driver** dropdown (top left):
- Choose from 1-20 (driver numbers)
- Updates all panels for that driver

### **3. Understand the Panels**

| Panel | What It Shows | What to Look For |
|-------|---------------|------------------|
| Speed Trace | Actual speed over race time | Anomalies = sudden drops |
| RPM Trace | Engine RPM over time | Consistent patterns = healthy |
| Residuals Heatmap | Prediction errors across all 20 cars | Red = high error (anomaly?) |
| Throttle Position | Driver input (0-1 scale) | Spikes = aggressive driving |
| Brake Pressure | Braking force applied | Sustained = late braking zones |
| Anomaly Events | Pie chart of alerts | Green = normal, Red = anomalies |

### **4. Time Window Controls**

Top right: Change time range
- **Last hour**: Real-time monitoring
- **Last 6 hours**: Full race view
- **Custom**: Pick exact times

---

## 🔌 Data Source Configuration

**InfluxDB** is auto-configured:
- **Name**: InfluxDB
- **URL**: http://influxdb:8086
- **Database**: pitwall
- **Status**: Connected ✓

To verify:
1. Click **Settings** (gear icon, top left)
2. Select **Data Sources**
3. Click **InfluxDB**
4. Click **Test** → Should show "Data source is working"

---

## 📥 Creating Custom Panels

### **Add a New Panel**

1. Click **+** button (top left of dashboard)
2. Select **Visualization**
3. Configure:
   - **Data Source**: InfluxDB
   - **Measurement**: f1.telemetry, f1.residuals, or f1.alerts
   - **Field**: speed, rpm, throttle, brake, gear, etc.
   - **Driver**: Select from dropdown

### **Example: Custom Speed Comparison**

1. Add new panel
2. Set data source to InfluxDB
3. Query: All drivers' speed
4. Visualization: **Graph** (multi-line to compare all 20)
5. Title: "Speed — All Drivers"

---

## 🔍 What Data Is Available?

Once the replayer streams data, these measurements appear in InfluxDB:

### **f1.telemetry**
```
Fields:
- speed (km/h)
- rpm (revolutions/min)
- throttle (0-1)
- brake (0-1)
- gear (1-8)
- drs (0-1)

Tags:
- driver (0-19)
- session_time
```

### **f1.residuals**
```
Fields:
- speed_residual (predicted - actual)
- rpm_residual
- throttle_residual
- brake_residual
- gear_residual

Tags:
- driver
- timestamp
```

### **f1.alerts**
```
Fields:
- alert_type (drift, anomaly, regime_change)
- severity (0-1)
- description

Tags:
- driver
- timestamp
```

---

## ⚙️ Customize Dashboard

### **Edit Dashboard**

1. Click **Edit** (pencil icon, top right)
2. Modify panels:
   - Click panel header → edit query
   - Change title, colors, legend
   - Resize panels (drag corners)
3. Click **Save** (top right)

### **Export Dashboard**

1. Click **Share** (top right)
2. Select **Export**
3. Copy JSON (useful for backup)

### **Import Dashboard**

1. Click **+** (left sidebar)
2. Select **Import**
3. Paste JSON or upload file
4. Select InfluxDB as data source
5. Click **Import**

---

## 🎨 Formatting Tips

### **Color Coding**

- **Green**: Normal operation
- **Yellow**: Warning (elevated but not critical)
- **Red**: Anomaly detected (requires attention)

Configure in panel:
1. Edit panel
2. Go to **Thresholds**
3. Set values:
   - Green < 10 (low residuals = good)
   - Yellow 10-50 (medium)
   - Red > 50 (high = anomaly)

### **Legends**

- **Show**: Display series names
- **Hide**: Cleaner view
- **Right**: Sidebar legend (good for many series)

---

## 📋 Useful Queries

### **Speed Over Time (One Driver)**

```
SELECT "speed" FROM "f1.telemetry" WHERE "driver" = '1'
```

### **RPM All Drivers**

```
SELECT "rpm" FROM "f1.telemetry" GROUP BY "driver"
```

### **High Residuals (Anomalies)**

```
SELECT "speed_residual" FROM "f1.residuals" 
WHERE "speed_residual" > 50
```

### **Drift Events**

```
SELECT * FROM "f1.alerts" 
WHERE "alert_type" = 'drift'
```

---

## 🚨 Troubleshooting

### **Dashboard Has No Data**

1. **Replayer not running?**
   - Run: `docker-compose run replayer python -m replayer.main`
   - Wait for data to stream into Kafka

2. **Data in Kafka but not InfluxDB?**
   - Spark streaming job needs to run (writes to InfluxDB)
   - For now, data is in Kafka, not InfluxDB
   - Workaround: Query Kafka topics directly in Grafana

3. **InfluxDB not connected?**
   - Check: Settings → Data Sources → InfluxDB
   - Click **Test** button
   - Verify InfluxDB is running: `docker-compose ps influxdb`

### **Dashboard Not Auto-Loading**

1. Restart Grafana: `docker-compose restart grafana`
2. Wait 10 seconds: `sleep 10`
3. Refresh browser: http://localhost:3000

---

## 📚 Next Steps

1. **Start replayer** (if not already running):
   ```bash
   docker-compose run replayer python -m replayer.main
   ```

2. **Watch Kafka UI** at http://localhost:8080 to see data flowing

3. **Check Grafana** at http://localhost:3000 for dashboards (once Spark writes to InfluxDB)

4. **Customize** panels to fit your needs

---

## 📞 Quick Commands

```bash
# View Grafana logs
docker-compose logs -f grafana

# Restart Grafana
docker-compose restart grafana

# Check if InfluxDB is getting data
docker exec streaming_influxdb_1 influx query 'SELECT COUNT(*) FROM "f1.telemetry"'
```

---

**Dashboards are ready to visualize your F1 telemetry data!** 🏁


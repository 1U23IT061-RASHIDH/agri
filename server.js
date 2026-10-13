/**
 * Real-Time Delivery GPS Tracking Service
 * Digital Agri Marketplace
 *
 * Architecture:
 * Driver Device -> Browser Geolocation API -> Socket.IO -> Real-Time Server -> Buyer Dashboard -> Modular Live Map
 */

const http = require("http");
const path = require("path");
const express = require("express");
const cors = require("cors");
const { Server } = require("socket.io");
require("dotenv").config();

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const STALE_TIMEOUT_MS = 30000; // 30 seconds without updates = stale/temporarily unavailable

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Socket.IO configuration with CORS support
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingTimeout: 10000,
  pingInterval: 5000
});

/**
 * Ephemeral In-Memory Delivery & Tracking Registry
 * Key: orderId
 * Value: {
 *   orderId,
 *   driverId,
 *   driverName,
 *   driverPhone,
 *   status: 'PENDING'|'ACCEPTED'|'PROCESSING'|'READY_FOR_PICKUP'|'OUT_FOR_DELIVERY'|'DELIVERED',
 *   trackingActive: boolean,
 *   lastLocation: {
 *     latitude: number,
 *     longitude: number,
 *     accuracy: number,
 *     speed: number,
 *     heading: number,
 *     timestamp: number
 *   } | null,
 *   destination: {
 *     address: string,
 *     latitude: number,
 *     longitude: number
 *   },
 *   updatedAt: number
 * }
 */
const activeDeliveries = new Map();

// Map driver socketId -> Set of orderIds they are actively streaming
const driverSockets = new Map();

// Helper to get or initialize delivery state
function getOrCreateDelivery(orderId, initialData = {}) {
  if (!activeDeliveries.has(orderId)) {
    activeDeliveries.set(orderId, {
      orderId,
      driverId: initialData.driverId || "user_demo_driver",
      driverName: initialData.driverName || "Ramesh Kumar (AgriExpress)",
      driverPhone: initialData.driverPhone || "+91 98765 01234",
      status: initialData.status || "READY_FOR_PICKUP",
      trackingActive: initialData.status === "OUT_FOR_DELIVERY",
      lastLocation: null,
      destination: initialData.destination || {
        address: "APMC Market Yard, Vashi, Navi Mumbai",
        latitude: 19.0760,
        longitude: 72.8777
      },
      updatedAt: Date.now()
    });
  }
  return activeDeliveries.get(orderId);
}

// REST API Endpoints
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    activeDeliveriesCount: activeDeliveries.size
  });
});

// Get delivery state and last known location
app.get("/api/deliveries/:orderId", (req, res) => {
  const { orderId } = req.params;
  const delivery = getOrCreateDelivery(orderId);
  res.json({
    success: true,
    delivery: {
      ...delivery,
      isStale: delivery.lastLocation
        ? Date.now() - delivery.lastLocation.timestamp > STALE_TIMEOUT_MS
        : true
    }
  });
});

// Assign driver to delivery
app.post("/api/deliveries/:orderId/assign", (req, res) => {
  const { orderId } = req.params;
  const { driverId, driverName, driverPhone } = req.body;
  if (!driverId) {
    return res.status(400).json({ error: "driverId is required" });
  }

  const delivery = getOrCreateDelivery(orderId);
  delivery.driverId = driverId;
  if (driverName) delivery.driverName = driverName;
  if (driverPhone) delivery.driverPhone = driverPhone;
  delivery.updatedAt = Date.now();

  io.to(`delivery:${orderId}`).emit("delivery_driver_assigned", {
    orderId,
    driverId: delivery.driverId,
    driverName: delivery.driverName,
    driverPhone: delivery.driverPhone
  });

  res.json({ success: true, delivery });
});

// Update delivery status
app.post("/api/deliveries/:orderId/status", (req, res) => {
  const { orderId } = req.params;
  const { status, actorId, role } = req.body;

  const validStatuses = [
    "PENDING",
    "ACCEPTED",
    "PROCESSING",
    "READY_FOR_PICKUP",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
  ];

  if (!validStatuses.includes(status)) {
    return res.status(400).json({ error: `Invalid status: ${status}` });
  }

  const delivery = getOrCreateDelivery(orderId);

  // Validate driver authorization if caller claims to be driver
  if (role === "DRIVER" && actorId && delivery.driverId && delivery.driverId !== actorId) {
    return res.status(403).json({ error: "Driver not authorized for this delivery" });
  }

  delivery.status = status;
  delivery.updatedAt = Date.now();

  if (status === "OUT_FOR_DELIVERY") {
    delivery.trackingActive = true;
  } else if (status === "DELIVERED") {
    delivery.trackingActive = false;
  }

  io.to(`delivery:${orderId}`).emit("delivery_status_changed", {
    orderId,
    status,
    trackingActive: delivery.trackingActive,
    updatedAt: delivery.updatedAt
  });

  res.json({ success: true, delivery });
});

// Socket.IO Real-Time Delivery Event Handling
io.on("connection", (socket) => {
  console.log(`[Socket Connected] id=${socket.id}`);

  /**
   * Client joins the real-time delivery channel
   * room: delivery:ORDER_ID
   */
  socket.on("join_delivery", (payload, callback) => {
    try {
      const { orderId, role, userId, driverInfo, destination } = payload || {};
      if (!orderId) {
        if (callback) callback({ error: "Missing orderId" });
        return;
      }

      const roomName = `delivery:${orderId}`;
      socket.join(roomName);
      console.log(`[Socket ${socket.id}] joined room: ${roomName} (role=${role})`);

      const delivery = getOrCreateDelivery(orderId, {
        driverId: driverInfo ? driverInfo.id : undefined,
        driverName: driverInfo ? driverInfo.name : undefined,
        driverPhone: driverInfo ? driverInfo.phone : undefined,
        destination: destination || undefined
      });

      // If driver joins, associate socket
      if (role === "DRIVER" && userId) {
        if (!driverSockets.has(socket.id)) {
          driverSockets.set(socket.id, new Set());
        }
        driverSockets.get(socket.id).add(orderId);
      }

      const responsePayload = {
        success: true,
        orderId,
        status: delivery.status,
        trackingActive: delivery.trackingActive,
        driverId: delivery.driverId,
        driverName: delivery.driverName,
        driverPhone: delivery.driverPhone,
        destination: delivery.destination,
        lastLocation: delivery.lastLocation,
        isStale: delivery.lastLocation
          ? Date.now() - delivery.lastLocation.timestamp > STALE_TIMEOUT_MS
          : true
      };

      // Notify subscriber with current snapshot
      socket.emit("delivery_init", responsePayload);
      if (callback) callback(responsePayload);
    } catch (err) {
      console.error("Error in join_delivery:", err);
      if (callback) callback({ error: err.message });
    }
  });

  /**
   * Driver sends actual device GPS location
   * Real coordinates from navigator.geolocation.watchPosition()
   */
  socket.on("driver_location", (data, callback) => {
    try {
      const {
        orderId,
        driverId,
        latitude,
        longitude,
        accuracy,
        speed,
        heading,
        altitude,
        timestamp
      } = data || {};

      if (!orderId) {
        if (callback) callback({ error: "Missing orderId" });
        return;
      }

      // Validate numeric coordinates
      const lat = Number(latitude);
      const lng = Number(longitude);
      if (isNaN(lat) || isNaN(lng) || lat < -90 || lat > 90 || lng < -180 || lng > 180) {
        if (callback) callback({ error: "Invalid GPS coordinates" });
        return;
      }

      const delivery = getOrCreateDelivery(orderId);

      // Validate driver assignment
      if (delivery.driverId && driverId && delivery.driverId !== driverId) {
        console.warn(`[Driver Unauthorized] Attempt by ${driverId} on order ${orderId} (assigned to ${delivery.driverId})`);
        if (callback) callback({ error: "Driver unauthorized for this delivery" });
        return;
      }

      // Associate driver if not yet set
      if (!delivery.driverId && driverId) {
        delivery.driverId = driverId;
      }

      const locationRecord = {
        latitude: lat,
        longitude: lng,
        accuracy: typeof accuracy === "number" ? Math.round(accuracy * 10) / 10 : null,
        speed: typeof speed === "number" ? Math.max(0, Math.round(speed * 3.6 * 10) / 10) : null, // km/h
        heading: typeof heading === "number" && !isNaN(heading) ? Math.round(heading) : null,
        altitude: typeof altitude === "number" ? Math.round(altitude) : null,
        timestamp: timestamp || Date.now(),
        driverId: driverId || delivery.driverId
      };

      delivery.lastLocation = locationRecord;
      delivery.updatedAt = Date.now();

      // Ensure delivery is marked as active tracking if out for delivery
      if (delivery.status === "OUT_FOR_DELIVERY") {
        delivery.trackingActive = true;
      }

      const roomName = `delivery:${orderId}`;

      // Broadcast live GPS coordinates to everyone in the delivery channel
      io.to(roomName).emit("location_update", {
        orderId,
        status: delivery.status,
        trackingActive: delivery.trackingActive,
        location: locationRecord
      });

      if (callback) callback({ success: true, receivedAt: Date.now() });
    } catch (err) {
      console.error("Error in driver_location:", err);
      if (callback) callback({ error: err.message });
    }
  });

  /**
   * Driver or farmer updates delivery status over socket
   */
  socket.on("update_delivery_status", (data, callback) => {
    try {
      const { orderId, status, actorId, role } = data || {};
      if (!orderId || !status) {
        if (callback) callback({ error: "Missing orderId or status" });
        return;
      }

      const delivery = getOrCreateDelivery(orderId);

      // Validate driver permissions
      if (role === "DRIVER" && actorId && delivery.driverId && delivery.driverId !== actorId) {
        if (callback) callback({ error: "Driver not authorized" });
        return;
      }

      delivery.status = status;
      delivery.updatedAt = Date.now();

      if (status === "OUT_FOR_DELIVERY") {
        delivery.trackingActive = true;
      } else if (status === "DELIVERED") {
        delivery.trackingActive = false;
      }

      const roomName = `delivery:${orderId}`;
      io.to(roomName).emit("delivery_status_changed", {
        orderId,
        status: delivery.status,
        trackingActive: delivery.trackingActive,
        updatedAt: delivery.updatedAt
      });

      if (callback) callback({ success: true, status: delivery.status });
    } catch (err) {
      console.error("Error in update_delivery_status:", err);
      if (callback) callback({ error: err.message });
    }
  });

  // Handle client disconnection
  socket.on("disconnect", () => {
    console.log(`[Socket Disconnected] id=${socket.id}`);
    if (driverSockets.has(socket.id)) {
      const orders = driverSockets.get(socket.id);
      orders.forEach((orderId) => {
        const delivery = activeDeliveries.get(orderId);
        if (delivery && delivery.trackingActive && delivery.status === "OUT_FOR_DELIVERY") {
          // Inform buyers that driver signal disconnected
          io.to(`delivery:${orderId}`).emit("driver_signal_lost", {
            orderId,
            message: "Driver device disconnected. Waiting for GPS signal..."
          });
        }
      });
      driverSockets.delete(socket.id);
    }
  });
});

// Periodic stale check (every 10 seconds)
setInterval(() => {
  const now = Date.now();
  activeDeliveries.forEach((delivery, orderId) => {
    if (delivery.trackingActive && delivery.lastLocation) {
      const elapsed = now - delivery.lastLocation.timestamp;
      if (elapsed > STALE_TIMEOUT_MS && !delivery._isReportedStale) {
        delivery._isReportedStale = true;
        io.to(`delivery:${orderId}`).emit("driver_signal_stale", {
          orderId,
          elapsedSeconds: Math.round(elapsed / 1000),
          message: "Location temporarily unavailable — waiting for driver GPS signal"
        });
      } else if (elapsed <= STALE_TIMEOUT_MS && delivery._isReportedStale) {
        delivery._isReportedStale = false;
      }
    }
  });
}, 10000);

// Start server
server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(` Digital Agri Marketplace Real-Time Service Active!`);
  console.log(` Web Server:    http://localhost:${PORT}`);
  console.log(` Live Tracking: http://localhost:${PORT}/delivery.html`);
  console.log(` Driver Portal: http://localhost:${PORT}/driver.html`);
  console.log(`=======================================================`);
});

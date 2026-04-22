# Parcel Tool — PDA Route Assignment and Exception Handling System

## Overview

Parcel Tool is an internal logistics operations system designed for real-time parcel processing, route assignment, and exception handling.

The system is available in both PDA and Web versions, with the PDA interface serving as the primary operational entry point for scan-based workflows.

---

## System Design

- **PDA Version (Primary Interface)**
  - Designed for real-time scanning and processing
  - Optimized for warehouse floor operations
  - Used for immediate routing and exception handling

- **Web Version (Secondary Interface)**
  - Supports batch processing and operational automation
  - Provides additional visibility and control for non-PDA workflows

Both versions share the same backend logic for routing, assignment, and exception handling.

---

## Key Features

### Route Assignment
- Automatic route assignment based on zipcode
- Uses Excel-based route configuration (`zipcode → route_no`)
- Supports dynamic updates via route file reload

---

### Exception Handling

#### Wrong Address
- Detects invalid or failed delivery cases
- Transfers parcels to designated exception batches
- Reassigns to exception drivers

#### Stored Parcels (213 / 230)
- Identifies stored or returned parcels
- Automatically routes to appropriate batch
- Assigns to designated processing drivers

---

### Driver Reassignment
- Dynamically reassigns parcels between drivers
- Supports both standard routing and exception scenarios

---

### Status Automation
- Automatically updates parcel status (e.g., 202 transition)
- Inserts operation logs for traceability
- Skips redundant updates based on status rules

---

### Real-Time Processing Engine
- Queue-based processing pipeline
- Background worker processes parcels sequentially
- Provides real-time status tracking via API

---

### Route Configuration
- Excel-based route table (`route.xlsx`)
- Supports:
  - zipcode mapping
  - enable/disable flags
- Hot reload via API

---

## System Architecture

- **Backend:** Flask
- **API Integration:** External logistics API
- **Data Source:** Excel (route mapping)
- **Processing Model:** Queue + background worker
- **Frontend:** PDA WebView + Web interface

---

## How It Works

1. User scans or inputs tracking numbers (PDA)
2. System retrieves parcel details via API
3. Determines processing path:
   - Standard routing
   - Wrong address handling
   - Stored parcel handling
4. Executes:
   - Route assignment
   - Driver reassignment
   - Status updates
5. Updates result panel in real time

---

## Example Workflow (PDA)

- Scan tracking number  
- System fetches parcel info  
- Automatically determines route or exception path  
- Assigns driver / transfers batch  
- Updates status and logs  
- Displays result instantly  

---

## Notes

- This is a public-safe version of an internal system
- API endpoints and credentials are not included
- Business rules are simplified for demonstration

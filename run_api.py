package.path = package.path .. [[;C:\Program Files (x86)\NCR\CSM2.0\ftp\PulseAgent\PST\Install\fluentBit\config\lua\?.lua;]]

local fc = require("fluentCommon")
local global = require("global")


local eventcodes = {
    ------------- SYSTEM EVENT ID's -------------------
    [12]="The operating system started",
    [13]="The operating system is shutting down",
    [41]="The system has rebooted without cleanly shutting down first. This error could be caused if the system stopped responding, crashed, or lost power unexpectedly.",
    [52]="HD bad blocks marked",
    [109]="The kernel power manager has initiated a shutdown transition.",
    [6005]="The Event log service was started.",
    [6006]="The Event log service was stopped.",
    [6008]="The previous system shutdown was unexpected.",
    ---------------- APPLICATION EVENT ID's --------------------
    [5]="PMLLog",
    [750]="DCSOps",
    [751]="DCSOps",
    [752]="DCSOps",
    [753]="DCSOps",
    [754]="DCSOps",
    [755]="DCSOps",
    [7206]="CRITICAL | A Disk Drive in RAID1 Volume is Degraded",
    [7001]="WARNING | A Disk Drive in the RAID1 Volume has been removed",
    [7209]="WARNING | Verification and Repair in progress for RAID Volume",
    [7207]="INFORMATION | A Disk Drive in RAID1 Volume, Rebuilding is complete",
    [7000]="FIXED | A Disk Drive in RAID1 Volume has been detected",
    [999]="INVESTIGATE | BOPC OFFLINE after Scheduled Reboot",
    [998]="FIXED | Scheduled System Restart Recovered",
}


id = {}


local function add_label(tbl, key, val)
    if val then tbl[key] = val end
end


function logs(tag, timestamp, record)
    local body = {}


    if tag == "winlog.log" then
        local tsGen = global.toTimestamp(record["TimeGenerated"])
        if (os.time(os.date("!*t")) - tsGen) > 86400 then return -1 end

        local eventcode = record["EventID"] % 65536
        local msg = eventcodes[eventcode]; if not msg then return -1 end
        if (record["Message"] or "") == "" then record["Message"] = msg end

        body["labels"] = {recordNumber = tostring(record["RecordNumber"]), channel = record["Channel"], sourceName = record["SourceName"], eventId = tostring(eventcode)}
        body["application"] = {computerName = record["computerName"],applicationName = record["SourceName"]}
        body["severity"] = record["EventType"]
        body["message"] = record["Message"]
        body["logName"] = "Winlog - " .. record["SourceName"]
        body["logType"] = "LOG_MESSAGE"
        return 1, timestamp, global.eventFormatter(record, body, "LOG")
    elseif (tag == "radiant-logs.log") then 
        body["application"] = {computerName = record["computerName"], applicationName = "SMTOOLS"}
        body["severity"]  = string.upper(record["severity"]) or "INFO"
        body["message"] = record["nodes"].."|"..record["id"].."|"..record["modules"].."|"..record["message"]
        body["logName"] = string.match(record["file_path"], "DCS_SMTOOLS.*%.log$")
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {}
    elseif (tag == "radiant-offlinealert.log") then
        body["application"] = {computerName = record["computerName"], applicationName = "OfflineAlert"}
        body["severity"]  = string.upper(record["severity"]) or "INFO"
        body["message"] = record["nodes"].."|"..record["id"].."|"..record["modules"].."|"..record["message"]
        body["logName"] = string.match(record["file_path"], "DCS_OfflineAlert.*%.log$")
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {["NodeID"] = record["node_id"], ["NodeIP"] = record["node_ip"]}

    elseif tag == "fuel.log" then
        if record["message"] == nil then return -1 end

        body["application"] = {computerName = global.computerName, applicationName = "POS", applicationVersion = "N/A"}
        body["correlationId"] = fc.uuid()
        body["logName"] = string.match(record["file"], "DCS_FuelPriceChange.*%.log$") or "FuelPriceChange"
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {}
        body["severity"] = string.upper(record["severity"] or "INFO")

        local currentId = record["id"]
        local state = record["state"]
        local entry = id[currentId]


        if state == "New" then
            id[currentId] = {record = record, product = record["product"], price = record["price"], timestamp = nil, warned = false}
            body["message"] = record["message"] or "No message provided"
            body["labels"]["PriceChangeID"] = currentId
            body["labels"]["ProductID"] = record["product"]
            body["labels"]["Price"] = record["price"]
            return 1, timestamp, global.eventFormatter(record, body, "LOG")
        end

        if state == "Posted" then
            if not entry then entry = {warned=false}; id[currentId]=entry end
            entry.record = record
            entry.product = record["product"] or entry.product
            entry.price = record["price"] or entry.price
            entry.timestamp = timestamp

            if entry.product then
                body["labels"]["ProductID"] = entry.product
                body["labels"]["Price"] = entry.price
            end
            body["labels"]["PriceChangeID"] = currentId
            body["status"] = "Posted"
            body["severity"] = "INFO"
            body["message"] = record["message"] or "Price change posted"
            return 1, timestamp, global.eventFormatter(record, body, "LOG")
        end

        if state == "Executed" or state == "Finished" then
            if entry and entry.product then
                body["labels"]["ProductID"] = entry.product
                body["labels"]["Price"] = entry.price
            end
            body["labels"]["PriceChangeID"] = currentId
            body["status"] = "Executed"
            body["severity"] = "OK"
            body["message"] = "Price change ID " .. currentId .. " has been executed successfully."
            id[currentId] = nil
            return 1, timestamp, global.eventFormatter(record, body, "LOG")
        end

        body["labels"]["PriceChangeID"] = currentId
        body["message"] = record["message"] or ("State: " .. tostring(state))
        return 1, timestamp, global.eventFormatter(record, body, "LOG")

    elseif tag == "heartbeat.log" then
        local recordArr = {}
        for entryId, entry in pairs(id) do
            if entry.timestamp and not entry.warned and
               (timestamp - entry.timestamp) > 600 then
                body[entryId] = {}
                body[entryId]["application"] = {computerName = global.computerName, applicationName = "POS",applicationVersion = "N/A"}
                body[entryId]["correlationId"] = fc.uuid()
                body[entryId]["logType"] = "LOG_MESSAGE"
                body[entryId]["severity"] = "WARN"
                body[entryId]["logName"] = string.match(entry.record.file,"DCS_FuelPriceChange.*%.log$") or "FuelPriceChange"
                body[entryId]["message"] = "Did not receive EXECUTED/FINISHED state within 10 minutes for ID " .. entryId .. "."
                body[entryId]["labels"] = {PriceChangeID = entryId}

                if entry.product then
                    body[entryId]["labels"]["ProductID"] = entry.product
                    body[entryId]["labels"]["Price"] = entry.price
                end

                table.insert(recordArr, global.eventFormatter(record, body[entryId], "LOG"))
                entry.warned = true
            end
        end

        if #recordArr == 0 then
            return -1
        else
            return 1, timestamp, recordArr
        end
    end
    local newRecord = global.eventFormatter(record, body, "LOG")
    return 1, timestamp, newRecord
end



package.path = package.path .. [[;C:\Program Files (x86)\NCR\CSM2.0\ftp\PulseAgent\PST\Install\fluentBit\config\lua\?.lua;]]
local fc = require('fluentCommon')
local global = require('global')

local eventcodes = {
    ------------- SYSTEM EVENT ID's -------------------
    [12]="The operating system started",
    [13]="The operating system is shutting down",
    [41]="The system has rebooted without cleanly shutting down first. This error could be caused if the system stopped responding, crashed, or lost power unexpectedly.",
    [52]="HD bad blocks marked",
    [109]="The kernel power manager has initiated a shutdown transition.",
    [6005]="The Event log service was started.",
    [6006]="The Event log service was stopped.",
    [6008]="The previous system shutdown was unexpected.",
    ---------------- APPLICATION EVENT ID's --------------------
    [5]="PMLLog",
    [750]="DCSOps",
    [751]="DCSOps",
    [752]="DCSOps",
    [753]="DCSOps",
    [754]="DCSOps",
    [755]="DCSOps",
    [7206]="CRITICAL | A Disk Drive in RAID1 Volume is Degraded",
    [7001]="WARNING | A Disk Drive in the RAID1 Volume has been removed",
    [7209]="WARNING | Verification and Repair in progress for RAID Volume",
    [7207]="INFORMATION | A Disk Drive in RAID1 Volume, Rebuilding is complete",
    [7000]="FIXED | A Disk Drive in RAID1 Volume has been detected",
    [999]="INVESTIGATE | BOPC OFFLINE after Scheduled Reboot",
    [998]="FIXED | Scheduled System Restart Recovered",
}

id = {}

function logs(tag, timestamp, record)
    local body = {}
    if (tag == "winlog.log") then
        local timeStamp = global.toTimestamp(record["TimeGenerated"])
        if (os.time(os.date("!*t")) - timeStamp) > 60 * 60 * 24 then --everything older than 1 day
            return -1, timestamp, record
        end
        local eventcode = record["EventID"] % 65536
        local message = eventcodes[eventcode]
        if message == nil then
            return -1, timestamp, record
        end

        if record["Message"] == "" then
            record["Message"] = message
        end
        body["labels"] = {recordNumber = tostring(record["RecordNumber"]), channel = record["Channel"], sourceName = record["SourceName"], eventId = tostring(eventcode)}
        body["application"] = {computerName = record["computerName"], applicationName = record["SourceName"]}
        body["severity"] = record["EventType"]
        body["message"] = record["Message"]
        body["logName"] = "Winlog - " .. record["SourceName"]
        body["logType"] = "LOG_MESSAGE"
    elseif (tag == "radiant-logs.log") then 
        body["application"] = {computerName = record["computerName"], applicationName = "SMTOOLS"}
        body["severity"]  = string.upper(record["severity"]) or "INFO"
        body["message"] = record["nodes"].."|"..record["id"].."|"..record["modules"].."|"..record["message"]
        body["logName"] = string.match(record["file_path"], "DCS_SMTOOLS.*%.log$")
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {}
    elseif (tag == "radiant-offlinealert.log") then
        body["application"] = {computerName = record["computerName"], applicationName = "OfflineAlert"}
        body["severity"]  = string.upper(record["severity"]) or "INFO"
        body["message"] = record["nodes"].."|"..record["id"].."|"..record["modules"].."|"..record["message"]
        body["logName"] = string.match(record["file_path"], "DCS_OfflineAlert.*%.log$")
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {["NodeID"] = record["node_id"], ["NodeIP"] = record["node_ip"]}
    elseif (tag == "fuel.log") then

        if record["message"] == nil then
            return -1
        end
        
        body["application"] = {computerName = global.computerName, applicationName = "POS", applicationVersion = "N/A"}
        body["correlationId"] = fc.uuid()
        body["logName"] = string.match(record["file"], "DCS_FuelPriceChange.*%.log$")
        body["logType"] = "LOG_MESSAGE"
        body["labels"] = {}
        if record["severity"] ~= nil then
            body["severity"]  = string.upper(record["severity"])
        else
            body["severity"] = "INFO"
        end

        if (record["state"] == "New") then   
            id[record["id"]] = {record = record, timestamp = nil}
            body["message"] = record["message"] or record["Message"] or "No message provided"
            local newRecord = global.eventFormatter(record, body, "LOG")
            return 1, timestamp, newRecord
        end

        if (record["state"] == "Posted") then
            if id[record["id"]] == nil then             -- if we don't have record with state "New" in memory, just use the record as posted. 
                id[record["id"]] = {record = record}    -- we don't have access to Product and Price if this is the case.
            end
            id[record["id"]]["timestamp"] = timestamp
        end

        local healthy = false
        -- if the record contains a healthy event, send it as such and remove it from the list 
        local currentId = record["id"] 
        if (record["state"] == "Executed" or record["state"] == "Finished") then
            body["message"] = "Price change ID " .. currentId .. " has been executed successfully."

            if id[currentId] ~= nil and id[currentId]["record"]["product"] ~= nil  then --if we have product and price, include them
                body["labels"]["ProductID"] = id[currentId]["record"]["product"]
                body["labels"]["Price"] = id[currentId]["record"]["price"]
            end

            body["labels"]["PriceChangeID"] = currentId

            id[currentId] = nil
            healthy = true
        end

        if not healthy then
            body["message"] = record["message"] or record["Message"] or "No message provided"
            body["severity"] = record["severity"] or "INFO"
            local newRecord = global.eventFormatter(record, body, "LOG")
            return 1, timestamp, newRecord
        else 
            local recordArr = {}
            local newRecord = global.eventFormatter(record, body, "LOG")
            table.insert(recordArr, newRecord)

            local newBody = {}
            newBody["application"] = {computerName = global.computerName, applicationName = "POS", applicationVersion = "N/A"}
            newBody["correlationId"] = fc.uuid()
            newBody["logName"] = string.match(record["file"], "DCS_FuelPriceChange.*%.log$")
            newBody["logType"] = "LOG_MESSAGE"
            newBody["message"] = record["message"] or record["Message"] or "No message provided"
            newBody["severity"] = record["severity"] or "INFO"

            local origRecord = global.eventFormatter(record, newBody, "LOG")
            table.insert(recordArr, origRecord)

            return 1, timestamp, recordArr
        end
    elseif (tag == "heartbeat.log") then -- check for and send unhealthy event for fuel price change
        local recordArr = {} 
        for entryId, entry in pairs(id) do
            if(entry["timestamp"] ~= nil) then
                local elapsedTime = timestamp - entry["timestamp"]
                if (elapsedTime > 600) then
                    body[entryId] = {}
                    body[entryId]["application"] = {computerName = global.computerName, applicationName = "POS", applicationVersion = "N/A"}
                    body[entryId]["correlationId"] = fc.uuid()
                    body[entryId]["logType"] = "LOG_MESSAGE"
                    body[entryId]["labels"] = {}
                    body[entryId]["message"] = "Did not receive EXECUTED/FINISHED state within 10 minutes for ID " .. entryId .. "."
                    body[entryId]["severity"] = "WARN"
                    body[entryId]["logName"] = string.match(entry["record"]["file"], "DCS_FuelPriceChange.*%.log$")

                    if id[entryId]["record"]["product"] ~= nil then 
                        body[entryId]["labels"]["ProductID"] = id[entryId]["record"]["product"]
                        body[entryId]["labels"]["Price"] = id[entryId]["record"]["price"]
                    end

                    body[entryId]["labels"]["PriceChangeID"] = entryId

                    table.insert(recordArr, global.eventFormatter(record, body[entryId], "LOG"))
                    id[entryId] = nil
                end
            end
        end

        if (#recordArr == 0) then
            return -1
        else
            return 1, timestamp, recordArr
        end
    end
    local newRecord = global.eventFormatter(record, body, "LOG")
    return 1, timestamp, newRecord
end

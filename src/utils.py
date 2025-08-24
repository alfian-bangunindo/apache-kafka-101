def transform_weather_data(data: dict):
    sensors = data["sensors"]
    uv_level = (
        "Low"
        if sensors["uv_index"] < 2
        else (
            "Moderate"
            if sensors["uv_index"] < 5
            else "High" if sensors["uv_index"] < 7 else "Very High"
        )
    )

    return {
        "device_id": data["device_id"],
        "timestamp": data["timestamp"],
        "latitude": data["location"]["latitude"],
        "longitude": data["location"]["longitude"],
        "temperature": sensors["temperature"],
        "humidity": sensors["humidity"],
        "wind_speed": sensors["wind_speed"],
        "wind_direction": sensors["wind_direction"],
        "precipitation_mm": sensors["precipitation"],
        "uv_index": sensors["uv_index"],
        "uv_level": uv_level,
    }

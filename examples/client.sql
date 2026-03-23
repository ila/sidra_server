pragma initialize_client;

INSERT INTO home_energy_logs (device_id, timestamp, indoor_temp, outdoor_temp, heating_on, energy_used_kwh, postal_code)
VALUES
    ('device_1', '2023-10-01 08:00:00', 21.5, 15.0, TRUE, 2.5, '12345'),
    ('device_2', '2023-10-01 08:05:00', 22.0, 14.5, TRUE, 3.0, '12345'),
    ('device_1', '2023-10-01 09:00:00', 21.0, 15.5, FALSE, 0.0, '12345');

pragma refresh(daily_energy_usage_user);

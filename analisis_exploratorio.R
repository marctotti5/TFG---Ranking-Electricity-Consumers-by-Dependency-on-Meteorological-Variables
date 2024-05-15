# 1. SEPARACIÓN ENTRE ESTACIONES (PREGUNTAR SI ES MEJOR HACER TAMBIÉN EN PRIMAVERA/OTOÑO): hecho

## CARGA DE LIBRERÍAS
#library(data.table)
#library(dtplyr)
library(dplyr, warn.conflicts = FALSE)
#library(doSNOW) 
#library(foreach)
#library(parallel)
#library(doParallel)
#library(sparklyr)
#library(tidyverse)
library(lubridate)
library(readr)
library(tidyr)
library(ggplot2)
#library(mice)
library(xts)
#library('Cairo')
#CairoWin(5,5)
#dev.off()
library(DataExplorer)
library(ggforce)

# Solucionar gráficos sin antialising de Rstudio
#trace(grDevices:::png, quote({
#if (missing(type) && missing(antialias)) {
#type <- "cairo-png"
#antialias <- "subpixel"
#}
#}), print = FALSE)

## CARGA DE DATOS: de google drive
#library(googledrive)
#temp <- tempfile(fileext = ".zip")
#dl <- drive_download(
#        as_id("1AiZda_1-2nwrxI8fLD0Y6e5rTg7aocv0"), path = temp, overwrite = TRUE)
#out <- unzip(temp, exdir = tempdir())
#bank <- read.csv(out[14], sep = ";")



## CARGA DE DATOS - PARTIDOS EN 112 TABLAS
# Obtener la lista de archivos CSV en la carpeta
#setwd("./Datos/archive/hourly_dataset_clean_ISHOME")
setwd("/home/rstudio-user/Datos")

#file_list <- list.files(path = "/hourly_dataset_clean", 
#pattern = "hourly_clean_block_.*\\.csv")
#file_list <- list.files()

# Crear una lista para almacenar los datos de los archivos CSV
#data_list <- list()



# Iterar sobre cada archivo CSV
#for (i in 1:length(file_list)) {
# Leer el archivo CSV y almacenarlo en la lista de datos
#        data_list[[i]] <- read_csv(file_list[i], 
#                                   col_types = cols(`energy_kwh_hh` = col_number(),
#                                                    lc_lid = col_character()))
#}

# Combinar todos los datos en un solo data frame
#datos_hourly <- do.call(rbind, data_list)
#rm(data_list)


## CARGA DE DATOS - UN SOLO ARCHIVO
datos_hourly <- read_csv("datos_hourly_clean_ishome_full.csv", 
                         col_types = cols(`energy_kwh_hh` = col_number(),
                                          lc_lid = col_character()))
datos_hourly$weather_code_wmo_code <- as.factor(datos_hourly$weather_code_wmo_code)

### Quitamos atípicos y Estandarizamos la energía para cada ID
### Calcula la media y la desviación estándar de la energía para cada ID
datos_hourly <- datos_hourly %>%
        group_by(lc_lid) %>%
        mutate(mean_energy = mean(energy_kwh_hh),
               sd_energy = sd(energy_kwh_hh)) %>%
        ungroup()
datos_hourly <- datos_hourly %>%
        mutate(energy_standardized = (energy_kwh_hh - mean_energy) / sd_energy)
datos_hourly$mean_energy <- NULL
datos_hourly$sd_energy <- NULL

#datos_hourly <- read_csv("./Datos/archive/hourly_dataset_clean/hourly_clean_block_10.csv")
minMax <- function(x) {
        (x - min(x, na.rm = T)) / (max(x, na.rm = T) - min(x, na.rm = T))
}

datos_hourly_agg_general_date_season <- datos_hourly %>% group_by(date = date(tstp)) %>% 
        summarise(across(where(is.numeric), mean)) #%>% 
        #mutate(avg_change_daily_consumption = energy_kwh_hh - lag(energy_kwh_hh),
               #energy_kwh_hh_minmax = minMax(energy_kwh_hh),
               #avg_change_daily_consumption_minmax = minMax(avg_change_daily_consumption))

maxim_temperature <- datos_hourly %>% group_by(date = date(tstp)) %>%
        summarise(max_temperature = max(temperature_2m_c, na.rm = TRUE))
maxim_temperature <- maxim_temperature$max_temperature
datos_hourly_agg_general_date_season$max_temperature <- maxim_temperature
rm(maxim_temperature)

datos_hourly_agg_general_date_season$season <- ifelse(month(datos_hourly_agg_general_date_season$date) %in% c(3, 4, 5), "Spring", 
                                                      ifelse(month(datos_hourly_agg_general_date_season$date) %in% c(6, 7, 8), "Summer", 
                                                             ifelse(month(datos_hourly_agg_general_date_season$date) %in% c(9, 10, 11), "Autumn", 
                                                                    "Winter")))

#datos_hourly_agg_date_lclid <- datos_hourly %>% group_by(date = date(tstp), lc_lid) %>% 
#ºsummarise(across(where(is.numeric), mean))

## --------------------------------------------------------------------------------------------------------
## ------------------------------------------- ANÁLISIS GENERAL -------------------------------------------
## --------------------------------------------------------------------------------------------------------

# Introducción: descripción variables
plot_intro(datos_hourly)
plot_missing(datos_hourly)
plot_bar(select(datos_hourly, -lc_lid), with = "is_home")
plot_histogram(datos_hourly)
plot_correlation(na.omit(datos_hourly), maxcat = 5L, type = "c")

# Número de hogares que participan en el estudio
datos_hourly_grouped <- datos_hourly %>% 
        group_by(date = date(tstp)) %>% 
        summarise(number_households = n_distinct(lc_lid))
ggplot(datos_hourly_grouped, aes(x = date)) + 
        geom_line(aes(y = number_households), linewidth = 0.7) +
        scale_y_continuous(
                name = "Número de hogares",
        )+
        labs(title = "Número de hogares participantes en el estudio", 
             x = "Fecha")




## --------------------------------------------------------------------------------------------------------
## ------------------------------------------- ANÁLISIS ENERGY --------------------------------------------
## --------------------------------------------------------------------------------------------------------


### Histogramas Energy
histograma_energia <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_histogram(aes(y = ..density..), color = "black", 
                       fill = "white", binwidth = 0.05) +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Distribución de Energía Consumida",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Energía Consumida / Hora (kWh)",
             y = "Frecuencia Relativa/Densidad") +
        theme(legend.position = "top")

datos_hourly$is_home_category <- factor(datos_hourly$is_home, levels = c("FALSE", "TRUE"), labels = c("Ausente", "En casa"))

histograma_consumo_casa_vs_ausente <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_histogram(aes(fill = is_home_category), color = "black", binwidth = 0.05) +
        facet_wrap(~is_home_category, ncol = 2) +
        labs(title = "Distribución muestral del Consumo Energético",
             subtitle = "En casa vs Ausente (zoom x2)",
             x = "Consumo de Energía (kWh)", 
             y = "Frecuencia Absoluta") + 
        theme(legend.position = "none",
              plot.title = element_text(hjust = 0.5),
              plot.subtitle = element_text(hjust = 0.5)) + xlim(c(0, 2))

## Densidades muestrales de Energy
### Por año
distribucion_energy_YEAR <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~year(tstp)) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "por año",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))

### Por estación
distribucion_energy_estacion <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~season) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "por estación del año",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))

### Por mes
datos_hourly$month <- month(datos_hourly$tstp, label = TRUE)
distribucion_energy_meses <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~month) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "por mes del año",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))
datos_hourly$month <- NULL

### Por día del mes
datos_hourly$monthday <- mday(datos_hourly$tstp)
distribucion_energy_dia_mes <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~monthday) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "por día del mes",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))
datos_hourly$monthday <- NULL

### Por día de la semana
datos_hourly$weekday <- factor(weekdays(datos_hourly$tstp), 
                               c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
distribucion_energy_dias <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~weekday) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "por días de la semana",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))

### Por fines de semana vs laborables
datos_hourly$weekend <- ifelse(datos_hourly$weekday %in% c("Satuday", "Sunday"), "Weekend", "Working Days")
datos_hourly$weekday <- NULL
distribucion_energy_finde <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~weekend) +
        labs(title = "Densidades muestrales del Consumo Energético",
             subtitle = "fin de semana | días laborales",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))
datos_hourly$weekend <- NULL

### Por horas
distribucion_energy_horas <- ggplot(data = datos_hourly, aes(x = energy_kwh_hh)) +
        geom_density(fill = "#FF6666", color = "black", binwidth = 0.05) +
        facet_wrap(~hour(tstp)) +
        labs(title = "Distribuciones muestrales del Consumo Energético por horas",
             subtitle = "por horas",
             x = "Consumo de Energía (kWh)", 
             y = "Densidad") + 
        theme(legend.position = "none") + xlim(c(0, 0.5))

## Barplot consumo promedio por horas (en casa vs ausente)
datos_hourly_agg_hour_general <- datos_hourly %>%
        group_by(hour = hour(tstp), is_home) %>% 
        summarise(across(where(is.numeric), mean))
datos_hourly_agg_hour_general$is_home_category <- factor(datos_hourly_agg_hour_general$is_home, levels = c("FALSE", "TRUE"), labels = c("Ausente", "En casa"))

barplot_promedio_consumo_horas <- ggplot(data = datos_hourly_agg_hour_general, aes(x = hour, y = energy_kwh_hh)) +
        geom_col(aes(fill = is_home_category)) +
        facet_wrap_paginate(~is_home_category, ncol = 1, nrow = 2) +
        labs(title = "Consumo energético promedio por hora",
             subtitle = "En casa vs Ausente",
             x = "Hora", 
             y = "Media muestral de Energía Consumida (kWh)") + 
        theme(legend.position = "none",
              plot.title = element_text(hjust = 0.5),
              plot.subtitle = element_text(hjust = 0.5))
datos_hourly_agg_hour_general$is_home_category <- NULL

### Boxplot Energy
boxplot_energia <- ggplot(data = datos_hourly, aes(x = season, y = energy_kwh_hh, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Energía Consumida por hora",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Energía Consumida (kWh)") + 
        theme(legend.position = "none")

boxplot_energia_ignoreoutliers <- ggplot(data = datos_hourly, aes(x = season, y = energy_kwh_hh, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Energía Consumida por hora (zoom)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Energía Consumida (kWh)") + 
        theme(legend.position = "none") + ylim(c(0, 0.3))

boxplot_energia_standarized <- ggplot(data = datos_hourly, aes(x = season, y = energy_standardized, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Energía Estandarizada Consumida por hora",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Energía Consumida (kWh) - Estandarizada") + 
        theme(legend.position = "none")

summary(datos_hourly$energy_kwh_hh)



### ------------------------------------- Time series Energy ---------------------------------------------
#### Creamos un objeto xts para poder acceder a analisis exploratorio básico y sencillo
#### Graficar la serie temporal (gráfico de línea): NO ESTACIONARIO
datos_hourly_agg <- datos_hourly %>% 
        group_by(datetime = floor_date(tstp, "1 hour")) %>% 
        summarise(across(where(is.numeric), mean))

energy_promedio_datetime <- aggregate(energy_kwh_hh ~ tstp, data = datos_hourly, FUN = mean)
energy_var_datetime <- aggregate(energy_kwh_hh ~ tstp, data = datos_hourly, FUN = var)
energy_standarized_var_datetime <- aggregate(energy_standardized ~ tstp, data = datos_hourly, FUN = var, na.action = na.omit)

grafico_ts_consumo_RAW_IDS <- ggplot(datos_hourly, aes(x = tstp, y = energy_kwh_hh, color = lc_lid)) + 
        geom_line(linewidth = 0.05) + ylim(c(0, 4)) +
        geom_line(data = energy_promedio_datetime, aes(y = energy_kwh_hh), color = "black", size = 0.5) +
        labs(title = "Consumo de Energía por hora (kWh)",
             subtitle = "Londres (2011-2014), a nivel horario por ID (consumo medio en negro)",
             x = "Fecha-hora", y = "Consumo de Energía (kWh)") + 
        theme(legend.position = "none")

grafico_ts_var_consumo_RAW_IDS <- ggplot(energy_var_datetime, aes(x = tstp, y = energy_kwh_hh)) + 
        geom_line(linewidth = 0.5) + ylim(c(0, 0.6)) +
        labs(title = "Varianza del Consumo de Energía por hora (kWh)",
             subtitle = "Londres (2011-2014), a nivel horario por ID",
             x = "Fecha-hora", y = "Varianza muestral del Consumo de Energía (kWh)") + 
        theme(legend.position = "none")

grafico_ts_var_consumo_standarized_IDS <- ggplot(energy_standarized_var_datetime, aes(x = tstp, y = energy_standardized)) + 
        geom_line(linewidth = 0.5)  + ylim(c(0, 0.6)) +
        labs(title = "Varianza del Consumo de Energía Estandarizada por hora (kWh)",
             subtitle = "Londres (2011-2014), a nivel horario por ID",
             x = "Fecha-hora", y = "Varianza muestral del Consumo de Energía (kWh)") + 
        theme(legend.position = "none")


grafico_ts_consumo_avg_DATETIME <- ggplot(datos_hourly_agg, aes(x = datetime)) + 
        geom_line(aes(y = energy_kwh_hh), linewidth = 0.05) + 
        labs(title = "Consumo de Energía promedio por hora (kWh)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Fecha-hora", y = "Consumo de Energía promedio (kWh)") + 
        theme(legend.position = "none")

datos_hourly_agg_general_date_lclid <- datos_hourly %>% group_by(date = date(tstp), lc_lid) %>% 
        summarise(across(where(is.numeric), mean))

energy_promedio_date <- aggregate(energy_kwh_hh ~ date, data = datos_hourly_agg_general_date_lclid, FUN = mean)

grafico_ts_consumo_avg_DATE <- ggplot(datos_hourly_agg_general_date_lclid) + 
        geom_line(aes(x = date, y = energy_kwh_hh, color = lc_lid),
                  size = 0.07, alpha = 0.1) +
        geom_line(data = energy_promedio_date, aes(x = date, y = energy_kwh_hh),
                  color = "black", size = 0.6) +
        labs(title = "Consumo Diario Promedio de Energía por Hogar vs Media (kWh)",
             subtitle = "Londres (2011-2014), a nivel diario (Promedio en negro)",
             x = "Fecha", y = "Energía (kWh)", color = "ID del Hogar") + 
        theme(legend.position = "none") + ylim(c(0, 4))


### Serie promedio de consumo por hora del dia (agrupado por dia de la semana)
library(forecast)
datos_hourly <- datos_hourly %>%
        mutate(dia_semana = factor(weekdays(tstp), levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")),
               hora = as.numeric(format(tstp, "%H")))
promedios <- datos_hourly %>%
        group_by(dia_semana, hora) %>%
        summarise(promedio_energia = mean(energy_kwh_hh))

serie_temporal_consumo_promedio_hora_dia <- ggplot(promedios, aes(x = hora, y = promedio_energia, 
                                                                  group = dia_semana, color = dia_semana)) +
        geom_line() +
        labs(x = "Hora", 
             y = "Promedio de energía consumida (kWh)", 
             title = "Promedio de energía consumida por día de la semana y hora",
             color = "Día de la Semana") +
        scale_x_continuous(breaks = seq(0, 23, by = 1))

### Serie promedio de consumo por hora de cada dia de la semana
promedios <- datos_hourly %>% 
        mutate(Time = format(tstp, '%u %H:%M:%S')) %>% 
        group_by(Time, dia_semana) %>% 
        summarize(energy_avg = mean(energy_kwh_hh, na.rm = T))
fecha_inicio <- as.POSIXct("2024-05-06 00:00:00")  
fecha_fin <- as.POSIXct("2024-05-12 23:59:59") 
fechas_hora_semana <- as.POSIXct(seq.POSIXt(from = fecha_inicio, to = fecha_fin, by = "hour"))
promedios$tstp <- fechas_hora_semana

grafico_energy_avg_hourweekday <- ggplot(data = promedios, aes(x = tstp, y = energy_avg)) + 
        geom_line() + 
        labs(x = "Hora", 
             y = "Promedio anual de energía consumida por hora (kWh)", 
             title = "Promedio de energía consumida por día de la semana y hora",
             color = "Día de la Semana") + 
        scale_x_datetime(name = "Día de la Semana", 
                         date_breaks = "1 day",
                         date_labels = "%A")


## ---------------------------------------------------------------------------------------------------------
## ----------------- Decomposición de la serie temporal (nivel hora) de Consumo ----------------------------
## ---------------------------------------------------------------------------------------------------------
# Periodos estacionales esperados: 
## Estacionalidad horaria: period = 24
## Estacionalidad semanal: period = 24*7
## Estacionalidad mensual: period = 24*30
## Estacionalidad trimestral: period = 24*360/4
## Estacionalidad cuatrimestral: period = 24*360/3
## Estacionalidad semestral: period = 24*360/2
library(plotly)
library(forecast)
msts_datoshourly <- msts(datos_hourly_agg$energy_kwh_hh, seasonal.periods = c(24, 24*7, 24*30), 
                         ts.frequency = 24*365)
mstl(msts_datoshourly, robust = TRUE) %>% autoplot() %>% ggplotly()


msts_datoshourly <- msts(datos_hourly_agg$energy_kwh_hh, seasonal.periods = c(24, 24*7, 24*30, 24*360/4), ts.frequency = 24*365)
mstl(msts_datoshourly, robust = TRUE) %>% autoplot()


## Decomposición de la serie temporal de Temperatura

## --------------------------------------------------------------------------------------------------------
## --------------------------------- ANÁLISIS VARIABLES METEOROLÓGICAS -------------------------------------
## ---------------------------------------------------------------------------------------------------------
weather_hourly <- read_csv("open-meteo-51.49N0.16W23m_lastupdated.csv")
weather_hourly$time <- as.POSIXct(weather_hourly$time, tz = "Europe/London")

#encontrar_tstp_faltantes_hourly(weather_hourly$time, min(prueba$tstp), min(prueba$tstp)) # NO HAY FECHAS FALTANTES
weather_hourly[!complete.cases(weather_hourly), ]
weather_hourly <- weather_hourly %>% janitor::clean_names()
weather_hourly <- weather_hourly %>% select(-c("wind_speed_100m_km_h", "wind_direction_100m", 
                                               "cloud_cover_low_percent", "cloud_cover_mid_percent",
                                               "cloud_cover_high_percent"))
weather_hourly$weather_code_wmo_code <- as.factor(weather_hourly$weather_code_wmo_code)
weather_hourly$season <- ifelse(month(weather_hourly$time) %in% c(3, 4, 5), "Spring", 
                                ifelse(month(weather_hourly$time) %in% c(6, 7, 8), "Summer", 
                                       ifelse(month(weather_hourly$time) %in% c(9, 10, 11), "Autumn", 
                                              "Winter")))
weather_hourly$season <- factor(weather_hourly$season, levels = c("Winter", "Spring", "Summer", "Autumn"))


## TEMPERATURA
histograma_temperatura <- ggplot(data = weather_hourly, aes(x = temperature_2m_c)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Temperaturas",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Temperatura (Cº)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_temperatura_season <- ggplot(data = weather_hourly, aes(x = season, y = temperature_2m_c, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Temperaturas",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Temperatura (Cº)") + 
        theme(legend.position = "none")

ushape_grafico_consumo_temperatura_avg <- ggplot(data = datos_hourly_agg_general_date_season, 
                                                 aes(x = temperature_2m_c, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Temperatura Diaria Promedio (Cº)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Temperatura Diaria Promedio")

ushape_grafico_consumo_temperatura_avg_households <- ggplot(data = datos_hourly_agg_date_lclid, 
                                                            aes(x = temperature_2m_c, y = energy_kwh_hh)) + 
        geom_smooth(aes(color = lc_lid), alpha = 0.3) + 
        geom_smooth(data = datos_hourly_agg_general_date_season, 
                    aes(x = temperature_2m_c, y = energy_kwh_hh), color = "black")
xlab("Temperatura Diaria Promedio (Cº)") + 
        theme(legend.position = "none",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Temperatura Diaria Promedio ")


ushape_grafico_consumo_temperatura_max <- ggplot(data = datos_hourly_agg_general_date_season, 
                                                 aes(x = max_temperature, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Temperatura Diaria Máxima (Cº)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Temperatura Diaria Máxima")

### Time series Consumo vs Temperatura
coeff <- round(max(datos_hourly_agg_general_date_season$temperature_2m_c, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
temperatureColor <- "#fc5203"
energycolor <- "#03c2fc"
grafico_ts_consumo_avg_DATE_temperatura <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=temperature_2m_c/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Temperatura Diaria Promedio (Cº)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Temperatura Promedio", 
             x = "Fecha")

coeff <- round(max(datos_hourly_agg_general_date_season$max_temperature, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_temperatura_max <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=max_temperature/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Temperatura Diaria Máxima (Cº)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Temperatura Máxima", 
             x = "Fecha")

## SENSACION TÉRMICA
histograma_sensacion_termica <- ggplot(data = weather_hourly, aes(x = apparent_temperature_c)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Sensación Térmica",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Sensación Térmica (Cº)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_sensaciontermica_season <- ggplot(data = weather_hourly, aes(x = season, y = apparent_temperature_c, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Sensación Térmica",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Sensación Térmica (Cº)") + 
        theme(legend.position = "none")
grafico_consumo_sensacion_termica <- ggplot(data = datos_hourly_agg_general_date_season, 
                                            aes(x = apparent_temperature_c, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Sensación Térmica Diaria Promedio (Cº)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Sensación Térmica Diaria Promedio")

coeff <- round(max(datos_hourly_agg_general_date_season$apparent_temperature_c, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_sensacion_termica <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=apparent_temperature_c/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Sensación Térmica Diaria Promedio (Cº)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Sensación Térmica Promedio", 
             x = "Fecha")

## ROCÍO
histograma_rocio <- ggplot(data = weather_hourly, aes(x = dew_point_2m_c)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Punto de Rocío",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Punto de Rocío (Cº)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_rocio_season <- ggplot(data = weather_hourly, aes(x = season, y = dew_point_2m_c, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Punto de Rocío",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Punto de Rocío (Cº)") + 
        theme(legend.position = "none")
grafico_consumo_rocio <- ggplot(data = datos_hourly_agg_general_date_season, 
                                aes(x = dew_point_2m_c, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Punto de Rocío (Cº)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Punto de Rocío")

coeff <- round(max(datos_hourly_agg_general_date_season$dew_point_2m_c, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))

grafico_ts_consumo_rocio <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=dew_point_2m_c/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Punto de Rocío Diario Promedio (Cº)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Sensación Térmica Promedio", 
             x = "Fecha")

## !!! revisar:  PRECIPITACIONES (LLUVIA Y NIEVE)
histograma_lluvia <- ggplot(data = weather_hourly, aes(x = rain_mm)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") + xlim(c(0, 2.5)) +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Precipitaciones",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Precipitaciones (mm)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_lluvia_season <- ggplot(data = weather_hourly, aes(x = season, y = rain_mm, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Precipitaciones",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Precipitaciones (mm)") + 
        theme(legend.position = "none")
grafico_consumo_lluvia <- ggplot(data = datos_hourly_agg_general_date_season, 
                                 aes(x = rain_mm, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Lluvia (mm)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Lluvia (mm)")
coeff <- round(max(datos_hourly_agg_general_date_season$rain_mm, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))

grafico_ts_consumo_lluvia <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=rain_mm/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Lluvia (mm)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Precipitaciones Promedio Diarias (mm)", 
             x = "Fecha")

## Nubosidad
histograma_nubosidad <- ggplot(data = weather_hourly, aes(x = cloud_cover_percent)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        #geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Nubosidad (%)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Nubosidad (%)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_nubosidad_season <- ggplot(data = weather_hourly, aes(x = season, y = cloud_cover_percent, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Nubosidad (%)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Nubosidad (%)") + 
        theme(legend.position = "none")

grafico_consumo_nubosidad <- ggplot(data = datos_hourly_agg_general_date_season, 
                                    aes(x = cloud_cover_percent, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Nubosidad (%)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Nubosidad Diaria Promedio (%)")

coeff <- round(max(datos_hourly_agg_general_date_season$cloud_cover_percent, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_nubosidad <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=cloud_cover_percent/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Nubosidad (%)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Nubosidad Promedio Diaria", 
             x = "Fecha")

## Nieve
histograma_nieve_cm <- ggplot(data = weather_hourly, aes(x = snowfall_cm)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        #geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Nieve (cm)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Nieve (cm)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_nieve_mm_season <- ggplot(data = weather_hourly, aes(x = season, y = snowfall_cm, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Nieve (cm)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Nieve (cm)") + 
        theme(legend.position = "none")

grafico_consumo_nieve_cm <- ggplot(data = datos_hourly_agg_general_date_season, 
                                   aes(x = snowfall_cm, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + xlim(c(0.000001, 0.2)) +
        geom_smooth(alpha = 0.3) + 
        xlab("Nieve (cm)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Nieve (cm)")

coeff <- max(datos_hourly_agg_general_date_season$snowfall_cm, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE)
grafico_ts_consumo_nieve_cm <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=snowfall_cm/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Nieve (cm)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Nieve Promedio Diaria", 
             x = "Fecha")

histograma_nieve_profundidad <- ggplot(data = weather_hourly, aes(x = snow_depth_m)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        #geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Profundidad de Nieve (m)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Profundidad de la Nieve (m)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_nieve_profundidad_season <- ggplot(data = weather_hourly, aes(x = season, y = snow_depth_m, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Profundidad de Nieve (m)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Profundidad de Nieve (m)") + 
        theme(legend.position = "none")

grafico_consumo_nieve_profundidad <- ggplot(data = datos_hourly_agg_general_date_season, 
                                            aes(x = snow_depth_m, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + xlim(c(0.000001, 0.2)) +
        geom_smooth(alpha = 0.3) + 
        xlab("Profundidad de la Nieve (m)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Profundidad de Nieve (m)")

coeff <- max(datos_hourly_agg_general_date_season$snow_depth_m, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE)
grafico_ts_consumo_profundidad_nieve_m <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=snow_depth_m/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Profundidad de Nieve (m)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Profundidad de Nieve Promedio Diaria", 
             x = "Fecha")

## VIENTO - direccion
histograma_direccion_viento <- ggplot(data = weather_hourly, aes(x = wind_direction_10m)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Dirección del Viento (º)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Dirección del Viento (º)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_direccion_viento_season <- ggplot(data = weather_hourly, aes(x = season, y = wind_direction_10m, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Dirección del Viento (º)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Dirección del Viento (º)") + 
        theme(legend.position = "none")

grafico_consumo_direccionviento <- ggplot(data = datos_hourly_agg_general_date_season, 
                                          aes(x = wind_direction_10m, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Dirección Promedio del Viento (º)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Dirección Promedio del Viento (º)")

coeff <- max(datos_hourly_agg_general_date_season$wind_direction_10m, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE)
grafico_ts_consumo_direccionviento <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=wind_direction_10m/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Dirección del Viento (º)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Dirección del Viento Promedio Diaria", 
             x = "Fecha")

## VIENTO - velocidad
histograma_velocidad_viento <- ggplot(data = weather_hourly, aes(x = wind_speed_10m_km_h)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Velocidad del Viento (km/h)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Velocidad del Viento (km/h)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_velocidad_viento_season <- ggplot(data = weather_hourly, aes(x = season, y = wind_speed_10m_km_h, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Velocidad del Viento (km/h)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Velocidad del Viento (km/h)") + 
        theme(legend.position = "none")
grafico_consumo_velocidadviento <- ggplot(data = datos_hourly_agg_general_date_season, 
                                          aes(x = wind_speed_10m_km_h, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Velocidad Promedio del Viento (km/h)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Velocidad diaria Promedio del Viento (km/h)")

coeff <- max(datos_hourly_agg_general_date_season$wind_speed_10m_km_h, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE)
grafico_ts_consumo_direccionviento <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=wind_speed_10m_km_h/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Velocidad del Viento (km/h)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Velocidad diaria Promedio del Viento (km/h)", 
             x = "Fecha")

## VIENTO - rachas
histograma_rachas_viento <- ggplot(data = weather_hourly, aes(x = wind_gusts_10m_km_h)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Rachas de Viento (km/h)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Rachas de Viento (km/h)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_rachas_viento_season <- ggplot(data = weather_hourly, aes(x = season, y = wind_gusts_10m_km_h, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Rachas de Viento (km/h)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Rachas de Viento (km/h)") + 
        theme(legend.position = "none")
grafico_consumo_rachas_viento <- ggplot(data = datos_hourly_agg_general_date_season, 
                                        aes(x = wind_gusts_10m_km_h, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Rachas de Viento (km/h)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Velocidad Promedio Rachas de Viento (km/h)")

coeff <- max(datos_hourly_agg_general_date_season$wind_gusts_10m_km_h, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE)
grafico_ts_consumo_direccionviento <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=wind_gusts_10m_km_h/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Rachas de Viento (km/h)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Rachas Promedio de Viento (km/h)", 
             x = "Fecha")



## PRESIÓN ATMOSFÉRICA
histograma_presion <- ggplot(data = weather_hourly, aes(x = pressure_msl_h_pa)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Presión atmosférica (hPa)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Presión atmosférica (hPa)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_presion_season <- ggplot(data = weather_hourly, aes(x = season, y = pressure_msl_h_pa, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Presión atmosférica (hPa)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Presión atmosférica (hPa)") + 
        theme(legend.position = "none")

vertical_line_data <- data.frame(x = c(990, 1020), 
                                 y = c(0.355, 0.355),
                                 label = c("Borrascas", "Anticiclones"))
grafico_consumo_presion_atmosferica <- ggplot(data = datos_hourly_agg_general_date_season, 
                                              aes(x = pressure_msl_h_pa, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) +
        xlab("Presión atmosférica Promedio (hPa)") + 
        theme(legend.position = "top", plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Presión atmosférica diaria Promedio") +
        geom_vline(aes(xintercept = 1013), alpha = 0.8, linetype = "dotdash") +
        geom_text(data = vertical_line_data, aes(x = x, y = y, label = label), 
                  vjust = -0.5, hjust = -0.5, color = "black", fontface = "bold", alpha = 0.7) +
        guides(color = guide_legend(title = "Leyenda")) + ylim(c(0.15, 0.37))

coeff <- round(max(datos_hourly_agg_general_date_season$pressure_msl_h_pa, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_presion <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line(aes(y = minMax(pressure_msl_h_pa)), size = 0.5, color = temperatureColor) + 
        geom_line(aes(y = minMax(energy_kwh_hh)), size = 0.5, color = energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~., name="Presión atmosférica Promedio (hPa)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Presión atmosférica Promedio (hPa)", 
             subtitle = "Datos Normalizados por MinMax",
             x = "Fecha")

datos_hourly$surface_pressure_h_pa <- NULL #no aporta información

## HUMEDAD DEL AIRE
### Humedad
histograma_humedad <- ggplot(data = weather_hourly, aes(x = relative_humidity_2m_percent)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Humedad (%)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Humedad (%)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_humedad_season <- ggplot(data = weather_hourly, aes(x = season, y = relative_humidity_2m_percent, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Humedad (%)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Humedad (%)") + 
        theme(legend.position = "none")

grafico_consumo_humedad <- ggplot(data = datos_hourly_agg_general_date_season, 
                                  aes(x = relative_humidity_2m_percent, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Humedad diaria Promedio (%)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Humedad diaria Promedio (%)")

coeff <- round(max(datos_hourly_agg_general_date_season$relative_humidity_2m_percent, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_humedad <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=relative_humidity_2m_percent/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Humedad diaria Promedio (%)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Humedad diaria Promedio (%)", 
             x = "Fecha")

### VPD
histograma_VPD <- ggplot(data = weather_hourly, aes(x = vapour_pressure_deficit_k_pa)) + 
        geom_histogram(aes(y=..density..), color = "black", fill = "white") +
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title = "Histograma de Déficit de Presión de Vapor (kPa)",
             subtitle = "Londres (2011-2014), a nivel horario",
             x = "Déficit de Presión de Vapor (kPa)",
             y = "Frecuencia Relativa/Densidad") + 
        theme(legend.position = "top")

boxplot_VPD_season <- ggplot(data = weather_hourly, aes(x = season, y = vapour_pressure_deficit_k_pa, fill = season)) + 
        geom_boxplot(alpha = 0.8) +
        labs(title = "Boxplot de Déficit de Presión de Vapor (kPa)",
             subtitle = "Londres (2011-2014), a nivel horario y por estación",
             x = "Estación del Año",
             y = "Déficit de Presión de Vapor (kPa)") + 
        theme(legend.position = "none")
grafico_consumo_VPD <- ggplot(data = datos_hourly_agg_general_date_season, 
                              aes(x = vapour_pressure_deficit_k_pa, y = energy_kwh_hh)) + 
        geom_point(aes(color = season)) + 
        geom_smooth(alpha = 0.3) + 
        xlab("Déficit de Presión de Vapor (kPa)") + 
        theme(legend.position = "top",
              plot.title = element_text(hjust = 0.5)) +
        ylab("Consumo Diario de Energía Promedio (kWh)") +
        ggtitle("Consumo Diario de Energía Promedio (kWh) vs \n Déficit de Presión de Vapor (kPa)")

coeff <- round(max(datos_hourly_agg_general_date_season$vapour_pressure_deficit_k_pa, na.rm = TRUE)/max(datos_hourly_agg_general_date_season$energy_kwh_hh, na.rm = TRUE))
grafico_ts_consumo_VPD <- ggplot(datos_hourly_agg_general_date_season, aes(x = date)) +
        geom_line( aes(y=vapour_pressure_deficit_k_pa/coeff), size=0.5, color=temperatureColor) + 
        geom_line( aes(y=energy_kwh_hh), size=0.5, color=energycolor) +
        scale_y_continuous(
                # Features of the first axis
                name = "Consumo Diario Promedio de Energía (kWh)",
                
                # Add a second axis and specify its features
                sec.axis = sec_axis(~.*coeff, name="Déficit de Presión de Vapor (kPa)")
        ) +
        theme(
                axis.title.y = element_text(color = energycolor, size=13),
                axis.title.y.right = element_text(color = temperatureColor, size=13)
        ) +
        labs(title = "Consumo Promedio de Energía vs Déficit de Presión de Vapor (kPa)", 
             x = "Fecha")


### CORRELACIONES ENTRE VARIABLES METEOROLÓGICAS
variables_meteorologicas_pca <- colnames(weather_hourly)
variables_meteorologicas_pca <- variables_meteorologicas_pca[(variables_meteorologicas_pca != "time") & (variables_meteorologicas_pca != "season")]
pca_datos_hourly_df <- na.omit(select(datos_hourly, all_of(variables_meteorologicas_pca)))

plot_scatterplot(pca_datos_hourly_df, by = "temperature_2m_c")


### PCA variables meteorológicas:
plot_prcomp(pca_datos_hourly_df, nrow = 2L, ncol = 2L, sampled_rows = 1000L)






### ----------------------------------- TRATAMIENTO DE DATOS ATÍPICOS -------------------------------------
#### Creo una función para detectar atípicos en todas la energía, y les imputo atípicos por cada ID y luego estandarizo

outlier_detect <- function(x){
        qntile <- quantile(x, probs=c(.25, .75))
        H <- 1.5 * IQR(x, na.rm = T)
        x[x < (qntile[1] - H)] <- caps[1]
        x[x > (qntile[2] + H)] <- caps[2]
        return(x)
}


















### ------------------------------------------------------------------------------------------------------
### ------------------------------------------ PENDIENTE -------------------------------------------------
### ------------------------------------------------------------------------------------------------------
#### CONTRASTES/INTERVALOS CONFIANZA BOOTSTRAP DE DIFERENCIA DE CONSUMO PROMEDIO X SEASON
#### CÁLCULO DEL RANKING DE DEPENDENCIA (ENERGY VS TEMPERATURA) - CORRELACIONES LINEALES VS MONOTONAS (SPEARMAN, ETC)
#### CÁLCULO DEL RANKING DE DEPENDENCIA (ENERGY VS PCA DE VARIABLES METEOROLOGICAS) - CORRELACIONES LINEALES VS MONOTONAS (SPEARMAN, ETC)
#### COMPARATIVA ENTRE LA VARIANZA DE LAS CORRELACIONES ESTIMADAS MEDIANTE ALGUN TIPO DE MÉTODO DE REMUESTREO Y ELECCION DEL RANKING FINAL
#### ESCRIBIR TODO





























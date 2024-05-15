# Cargamos librerías
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
#library(mice)
#library(xts)

# 0. LECTURA Y LIMPIEZA DE DATOS
#library(googledrive)
#ls_tibble <- googledrive::drive_ls("TFG/halfhourly_raw")
#for (file_id in ls_tibble$id) {
#googledrive::drive_download(as_id(file_id))
#}


## Carpeta de entrada: input de datos raw (medio-horarios)
## Carpeta de salida: output de datos limpios (horarios)
setwd("C:/Users/marct/Documents/UNI_ESTADISTICA/4o/SEGUNDO CUATRIMESTRE/TFG/tfg_marc/TFG---Marc-Pastor")
carpeta_entrada <- "./Datos/archive/halfhourly_dataset/halfhourly_dataset_raw"
carpeta_salida <- "./Datos/archive/hourly_dataset_clean"

##  Obtener la lista de archivos en la carpeta de entrada
archivos <- list.files(carpeta_entrada, full.names = TRUE)
#archivos <- archivos[1:4]
#archivo <- archivos

## Obtener los datos meteorológicos, de households y demás
informations_households <- read_csv("./Datos/archive/informations_households.csv")
uk_bank_holidays <- read_csv("./Datos/archive/uk_bank_holidays.csv") %>% rename(date = `Bank holidays`)
#weather_hourly <- read_csv("./Datos/archive/weather_hourly_darksky.csv")
#weather_hourly$time <- as.POSIXct(weather_hourly$time, tz = "Europe/London")

# Obtenemos los datos meteorológicos (USAMOS OPENMETEO Q NO TIENE NA'S Y DA + INFO):
## - AQUI: nose pq hay diferente numero de filas, ver mñn
weather_hourly <- read_csv("C:/Users/marct/Documents/UNI_ESTADISTICA/4o/SEGUNDO CUATRIMESTRE/TFG/tfg_marc/TFG---Marc-Pastor/Datos/archive/open-meteo-51.49N0.16W23m_lastupdated.csv")
weather_hourly$time <- as.POSIXct(weather_hourly$time, tz = "Europe/London")

#encontrar_tstp_faltantes_hourly(weather_hourly$time, min(prueba$tstp), min(prueba$tstp)) # NO HAY FECHAS FALTANTES
weather_hourly[!complete.cases(weather_hourly), ]
weather_hourly <- weather_hourly %>% janitor::clean_names()
weather_hourly <- weather_hourly %>% select(-c("wind_speed_100m_km_h", "wind_direction_100m", 
                                               "cloud_cover_low_percent", "cloud_cover_mid_percent",
                                               "cloud_cover_high_percent"))
weather_hourly$weather_code_wmo_code <- as.factor(weather_hourly$weather_code_wmo_code)
# weather_code: documentacion sobre los WMO weather codes


### Miramos si hay horas faltantes por cada ID, filtrando fuera primero las fechas en las que se cambió de hora por horario de invierno/verano
`%notin%` <- Negate(`%in%`) 

encontrar_tstp_faltantes <- function(tstp_reales, min_tstp, max_tstp) {
  # Identificar las fechas de cambio de hora por horario de verano o invierno
  fechas_cambio_hora <- seq(as.POSIXct("2012-03-25 01:00:00", tz = "Europe/London"), 
                            as.POSIXct("2013-03-31 01:00:00", tz = "Europe/London"), 
                            by = "1 day")
  
  # Crear una secuencia de todas las horas desde el min_tstp hasta el max_tstp, cada media hora
  horas_esperadas <- seq.POSIXt(from = min_tstp, to = max_tstp, by = "30 min")
  
  # Eliminar las fechas de cambio de hora
  horas_esperadas <- horas_esperadas[!horas_esperadas %in% fechas_cambio_hora]
  
  # Encontrar las tstp faltantes
  horas_esperadas[horas_esperadas %notin% tstp_reales]
}

# Definir una función para imputar la media de cada NA como la media del mes en cuestión para ese ID concreto
imputar_grupo <- function(grupo) {
  lc_lid <- grupo$lc_lid
  month <- grupo$month
  
  datos_grupo <- half_hourly_dataset[half_hourly_dataset$lc_lid == lc_lid & 
                                       month(half_hourly_dataset$tstp) == month, ]
  
  # Realizar la imputación dentro del grupo (por ejemplo, usando la media)
  datos_grupo$energy_kwh_hh <- ifelse(is.na(datos_grupo$energy_kwh_hh),
                                      mean(datos_grupo$energy_kwh_hh, na.rm = TRUE),
                                      datos_grupo$energy_kwh_hh)
  
  # Si no hubiera suficientes datos para ese id, nos daría NA, así que imputaríamos la media general de ese mes del resto de id's
  
  return(datos_grupo)
}

## Volvemos a comprobar que no falten tstps y que no haya NA's
encontrar_tstp_faltantes_hourly <- function(tstp_reales, min_tstp, max_tstp) {
  # Identificar las fechas de cambio de hora por horario de verano o invierno
  fechas_cambio_hora <- seq(as.POSIXct("2012-03-25 01:00:00", tz = "Europe/London"), 
                            as.POSIXct("2013-03-31 01:00:00", tz = "Europe/London"), 
                            by = "1 day")
  
  # Crear una secuencia de todas las horas desde el min_tstp hasta el max_tstp, cada media hora
  horas_esperadas <- seq.POSIXt(from = min_tstp, to = max_tstp, by = "60 min")
  
  # Eliminar las fechas de cambio de hora
  horas_esperadas <- horas_esperadas[!horas_esperadas %in% fechas_cambio_hora]
  
  # Encontrar las tstp faltantes
  horas_esperadas[horas_esperadas %notin% tstp_reales]
}

tstp_faltantes <- function(df){
  # Función para encontrar timestamps faltantes por cada id
  df %>%
    group_by(lc_lid) %>%
    summarise(min_tstp = min(tstp, na.rm = TRUE),
              max_tstp = max(tstp, na.rm = TRUE),
              tstp_faltantes = list(encontrar_tstp_faltantes(tstp, min_tstp, max_tstp))) %>%
    mutate(num_tstps_faltantes = unlist(lapply(tstp_faltantes, length)))
}
tstp_faltantes_hourly <- function(df){
  # Función para encontrar timestamps faltantes por cada id
  df %>%
    group_by(lc_lid) %>%
    summarise(min_tstp = min(tstp, na.rm = TRUE),
              max_tstp = max(tstp, na.rm = TRUE),
              tstp_faltantes = list(encontrar_tstp_faltantes_hourly(tstp, min_tstp, max_tstp))) %>%
    mutate(num_tstps_faltantes = unlist(lapply(tstp_faltantes, length)))
}

# Lista con el porcentaje de na's de cada bloque
lista_pct_na <- vector(mode = "list", length = length(archivos))
names(lista_pct_na) <- archivos





#archivos <- archivos[1:3]

## Leemos solamente el primer archivo para probar y luego generalizar
for (archivo in archivos){
  half_hourly_dataset <- read_csv(archivo, col_types = cols(tstp = col_datetime(format = "%Y-%m-%d %H:%M:%S"),
                                                            `energy(kWh/hh)` = col_number(),
                                                            LCLid = col_character()))
  # -------- 0.1 ARCHIVOS HOURLY CLEAN --------------
  
  ## Miramos la estructura del dataframe
  # skimr::skim(half_hourly_dataset)
  
  ## Extraemos el % de NA's de cada bloque
  na_pct <- sum(!complete.cases(half_hourly_dataset))/nrow(half_hourly_dataset) * 100
  lista_pct_na[[archivo]] <- na_pct
  
  ## Pipeline de Limpieza
  half_hourly_dataset <- half_hourly_dataset %>% 
    
    # Convertimos tstp en POSIXCT
    mutate(tstp = as.POSIXct(tstp, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London")) %>%
    
    # Limpiamos y estandarizamos los nombres de columnas
    janitor::clean_names() %>% 
    
    # Quitamos NA's
    na.exclude() %>% 
    
    # Quitamos duplicados puros
    distinct() %>%
    
    # Ordenamos por fecha ascendente
    arrange(tstp) %>%
    
    # Cambiamos el nombre de columna a "energy_kwh_hh"
    rename(energy_kwh_hh = energy_k_wh_hh)
  
  # Encontrar las tstp faltantes para cada ID
  tstp_faltantes_por_id <- tstp_faltantes(half_hourly_dataset)
  
  # vemos que cada ID tiene diferentes fechas faltantes, utilizaremos imputación para eliminar esas fechas faltantes
  #print(tstp_faltantes_por_id) 
  
  
  # Crear un data frame con las filas faltantes para cada ID
  filas_faltantes <- tstp_faltantes_por_id %>%
    ungroup() %>%
    tidyr::unnest(tstp_faltantes) %>%
    mutate(energy_kwh_hh = NA_real_) %>% # Asignar NA a la columna energy_kwh_hh 
    rename(tstp = tstp_faltantes) %>% select(lc_lid, tstp, energy_kwh_hh)
  
  # Unir el data frame de filas faltantes con el conjunto de datos original
  half_hourly_dataset <- bind_rows(half_hourly_dataset, filas_faltantes) %>% arrange(tstp)
  
  # Ahora, todas las fechas faltantes tendrán NA en la variable energy_kwh_hh
  
  # Volvemos a comprobar si hay fechas faltantes: ahora NO hay
  tstp_faltantes_por_id <- tstp_faltantes(half_hourly_dataset)
  if(sum(tstp_faltantes_por_id$num_tstps_faltantes) > 0){
    stop(paste("Hay Fechas Faltantes en", substr(archivo, 59, nchar(archivo))))
  } 
  
  
  ### Imputación mediante de los datos faltantes, mediante la media, pq sino tardaba mucho y son muchos datos
  # Obtener los grupos ID-mes
  grupo1 <- unique(half_hourly_dataset$lc_lid)
  grupo2 <- unique(month(half_hourly_dataset$tstp))
  
  grupos <- expand.grid(lc_lid = unique(half_hourly_dataset$lc_lid), 
                        month = unique(month(half_hourly_dataset$tstp)))
  
  # Imputar dentro de cada grupo los NA' mediante la media
  datos_imputados <- lapply(split(grupos, seq(nrow(grupos))), imputar_grupo)
  datos_imputados <- do.call(rbind, datos_imputados)
  rm(half_hourly_dataset)
  
  # Ahora si aún hay NA's imputamos la media global
  if(nrow(datos_imputados[!complete.cases(datos_imputados$energy_kwh_hh), ]) > 0){
    datos_imputados[!complete.cases(datos_imputados$energy_kwh_hh), ]$energy_kwh_hh <- mean(datos_imputados$energy_kwh_hh, na.rm = TRUE)
  }
  
  # Ahora que tenemos los datos sin NA's y con todos los timestamps, estamos en posición de agregar a nivel hora
  # Borramos los archivos medio/horarios para ahorrar memoria
  gc()
  
  datos_imputados <- datos_imputados %>%
    mutate(hora = floor_date(tstp, unit = "hour")) %>%
    mutate(tstp = as.POSIXct(tstp, tz = "Europe/London"))
  
  # Agregamos los datos a nivel hora
  datos_hourly <- datos_imputados %>%
    group_by(lc_lid, hora) %>%
    summarize(energy_kwh_hh_promedio = mean(energy_kwh_hh, na.rm = TRUE)) %>%
    rename(tstp = hora, energy_kwh_hh = energy_kwh_hh_promedio) %>%
    select(lc_lid, tstp, energy_kwh_hh) %>% 
    arrange(tstp)
  rm(datos_imputados)
  
  # Encontrar las tstp faltantes para cada ID
  tstp_faltantes_por_id_hourly <- tstp_faltantes_hourly(datos_hourly)
  if(sum(tstp_faltantes_por_id_hourly$num_tstps_faltantes) > 0){
    stop(paste("Hay Fechas Faltantes en", substr(archivo, 59, nchar(archivo))))
  } 
  # tstp_faltantes_por_id_hourly # vemos que no faltan tstp a nivel horario
  
  
  ## Hacer el join con datos meteorológicos y demás
  datos_hourly <- left_join(datos_hourly, weather_hourly, join_by(tstp == time))
  
  ## Probar con el otro dataset de weather porque este tiene algunos NA's en pressure
  
  ## Join con us_bank_holidays
  datos_hourly$date <- date(datos_hourly$tstp)
  datos_hourly <- left_join(datos_hourly, uk_bank_holidays, join_by(date == date)) %>% 
    select(-date)
  
  ## Join con households
  datos_hourly <- left_join(datos_hourly, informations_households, join_by(lc_lid == LCLid))
  
  ## Columna de estación (season):
  datos_hourly$season <- ifelse(month(datos_hourly$tstp) %in% c(3, 4, 5), "Spring", 
                                ifelse(month(datos_hourly$tstp) %in% c(6, 7, 8), "Summer", 
                                       ifelse(month(datos_hourly$tstp) %in% c(9, 10, 11), "Autumn", 
                                              "Winter")))
  
  ## Comprobamos q no hay NA's aparte de la columna Type que indica el tipo de festivo
  #prueba2 <- datos_hourly %>% select(-Type)
  #prueba2[!complete.cases(prueba2), ]
  
  # Guardar el archivo limpio (general, sin dividir x estaciones) en la carpeta de salida
  nombre_archivo_salida <- paste("hourly_clean", substr(archivo, 67, nchar(archivo)), sep = "_")
  write_csv(datos_hourly, paste(carpeta_salida, nombre_archivo_salida, sep = "/"))
  rm(datos_hourly)
  
  # -------- 0.2 ARCHIVOS HOURLY POR ESTACIONES --------------
  gc()
  ## Invierno
  #datos_winter <- datos_hourly %>% filter(season == "Winter")
  #nombre_archivo_salida_winter <- paste("hourly_clean_winter", substr(archivo, 59, nchar(archivo)), sep = "_")
  #carpeta_salida_winter <- paste0(carpeta_salida, "/winter")
  #write_csv(datos_winter, paste(carpeta_salida_winter, nombre_archivo_salida_winter, sep = "/"))
  #gc()
  ## Primavera
  #datos_spring <- datos_hourly %>% filter(season == "Spring")
  #nombre_archivo_salida_spring <- paste("hourly_clean_spring", substr(archivo, 59, nchar(archivo)), sep = "_")
  #carpeta_salida_spring <- paste0(carpeta_salida, "/spring")
  #write_csv(datos_spring, paste(carpeta_salida_spring, nombre_archivo_salida_spring, sep = "/"))
  #gc()
  ## Verano
  #datos_summer <- datos_hourly %>% filter(season == "Summer")
  #nombre_archivo_salida_summer <- paste("hourly_clean_summer", substr(archivo, 59, nchar(archivo)), sep = "_")
  #carpeta_salida_summer <- paste0(carpeta_salida, "/summer")
  #write_csv(datos_summer, paste(carpeta_salida_summer, nombre_archivo_salida_summer, sep = "/"))
  #gc()
  ## Otoño
  #datos_autumn <- datos_hourly %>% filter(season == "Autumn")
  #nombre_archivo_salida_autumn <- paste("hourly_clean_autumn", substr(archivo, 59, nchar(archivo)), sep = "_")
  #carpeta_salida_autumn <- paste0(carpeta_salida, "/autumn")
  #write_csv(datos_autumn, paste(carpeta_salida_autumn, nombre_archivo_salida_autumn, sep = "/"))
  #gc()
  
  
}











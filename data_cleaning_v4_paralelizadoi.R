# Instala parallel si no lo tienes instalado
# install.packages("parallel")

# Carga los paquetes necesarios
library(dplyr, warn.conflicts = FALSE)
library(lubridate)
library(readr)
library(tidyr)
library(parallel)

## Carpeta de entrada: input de datos raw (medio-horarios)
## Carpeta de salida: output de datos limpios (horarios)
#setwd("C:/Users/marct/Documents/UNI_ESTADISTICA/4o/SEGUNDO CUATRIMESTRE/TFG/tfg_marc/TFG---Marc-Pastor")
carpeta_entrada <- "./Datos/archive/halfhourly_dataset/halfhourly_dataset_raw"
carpeta_salida <- "./Datos/archive/hourly_dataset_clean"

##  Obtener la lista de archivos en la carpeta de entrada
archivos <- list.files(carpeta_entrada, full.names = TRUE)


## Obtener los datos meteorológicos, de households y demás
informations_households <- read_csv("./Datos/archive/informations_households.csv")
#uk_bank_holidays <- read_csv("./Datos/archive/uk_bank_holidays.csv") %>% rename(date = `Bank holidays`)


# Obtenemos los datos meteorológicos:
weather_hourly <- read_csv("./Datos/archive/open-meteo-51.49N0.16W23m_lastupdated.csv")
weather_hourly$time <- as.POSIXct(weather_hourly$time, tz = "Europe/London")


weather_hourly[!complete.cases(weather_hourly), ]
weather_hourly <- weather_hourly %>% janitor::clean_names()
weather_hourly <- weather_hourly %>% select(-c("wind_speed_100m_km_h", "wind_direction_100m", 
                                               "cloud_cover_low_percent", "cloud_cover_mid_percent",
                                               "cloud_cover_high_percent"))
weather_hourly$weather_code_wmo_code <- as.factor(weather_hourly$weather_code_wmo_code)
# weather_code: documentacion sobre los WMO weather codes


### Miramos si hay horas faltantes por cada ID, filtrando fuera primero las fechas en las que se cambió de hora por horario de invierno/verano


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

#encontrar_tstp_faltantes_hourly(weather_hourly$time, min(prueba$tstp), min(prueba$tstp)) # NO HAY FECHAS FALTANTES EN DATOS METEOROLÓGICOS


# Definir una función para imputar la media de cada NA como la media de la hora en cuestión para ese ID concreto
imputar_grupo <- function(grupo, half_hourly_dataset) {
  lc_lid <- grupo$lc_lid
  hour <- grupo$hour
  
  datos_grupo <- half_hourly_dataset[half_hourly_dataset$lc_lid == lc_lid & 
                                       hour(half_hourly_dataset$tstp) == hour, ]
  
  # Realizar la imputación dentro del grupo (por ejemplo, usando la media)
  datos_grupo$energy_kwh_hh <- ifelse(is.na(datos_grupo$energy_kwh_hh),
                                      mean(datos_grupo$energy_kwh_hh, na.rm = TRUE),
                                      datos_grupo$energy_kwh_hh)
  
  # Si no hubiera suficientes datos para ese id, nos daría NA, así que imputaríamos la media general de ese mes del resto de id's
  
  return(datos_grupo)
}



`%notin%` <- Negate(`%in%`) 

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

is_outlier <- function(x) {
  Q1 <- quantile(x, 0.25, na.rm = TRUE)
  Q3 <- quantile(x, 0.75, na.rm = TRUE)
  IQR <- Q3 - Q1
  (x < (Q1 - 1.5 * IQR)) | (x > (Q3 + 1.5 * IQR))
}


procesar_archivo <- function(archivo, carpeta_salida, weather_hourly, informations_households) {
  # Leer el archivo
  half_hourly_dataset <- read_csv(archivo, col_types = cols(tstp = col_datetime(format = "%Y-%m-%d %H:%M:%S"),
                                                            `energy(kWh/hh)` = col_number(),
                                                            LCLid = col_character()))
  
  # Limpiar y procesar los datos
  half_hourly_dataset <- half_hourly_dataset %>%
    mutate(tstp = as.POSIXct(tstp, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London")) %>%
    janitor::clean_names() %>%
    na.exclude() %>%
    distinct() %>%
    arrange(tstp) %>%
    rename(energy_kwh_hh = energy_k_wh_hh)
  
  # Encontrar las tstp faltantes para cada ID
  tstp_faltantes_por_id <- tstp_faltantes(half_hourly_dataset)
  
  # Crear un data frame con las filas faltantes para cada ID
  filas_faltantes <- tstp_faltantes_por_id %>%
    ungroup() %>%
    tidyr::unnest(tstp_faltantes) %>%
    mutate(energy_kwh_hh = NA_real_) %>%
    rename(tstp = tstp_faltantes) %>%
    select(lc_lid, tstp, energy_kwh_hh)
  
  # Unir el data frame de filas faltantes con el conjunto de datos original
  half_hourly_dataset <- bind_rows(half_hourly_dataset, filas_faltantes) %>%
    arrange(tstp)
  
  ## Extraemos el % de NA's de cada hogar de cada bloque, 
  pct_na_hogar <- half_hourly_dataset %>%
    group_by(lc_lid) %>%
    summarize(pct_na = sum(is.na(energy_kwh_hh)) / n())
  pct_na_hogar$eliminar_por_exceso_nas <- ifelse(pct_na_hogar$pct_na > 0.1, TRUE, FALSE)
  
  ## Eliminamos aquellos hogares con + del 10% de NA's, y los guardamos en un dataset block_num_ids_removed_highNA
  half_hourly_dataset <- half_hourly_dataset %>% filter(lc_lid %notin% pct_na_hogar[pct_na_hogar$eliminar_por_exceso_nas, ]$lc_lid)
  
  
  # Obtener los grupos ID-mes
  grupos <- expand.grid(lc_lid = unique(half_hourly_dataset$lc_lid), 
                        hour = unique(hour(half_hourly_dataset$tstp)))
  
  # Imputar dentro de cada grupo los NA's mediante la media
  datos_imputados <- lapply(split(grupos, seq(nrow(grupos))), function(grupo) imputar_grupo(grupo, half_hourly_dataset))
  datos_imputados <- do.call(rbind, datos_imputados)
  
  # Imputar la media global si aún hay NA's
  if(nrow(datos_imputados[!complete.cases(datos_imputados$energy_kwh_hh), ]) > 0){
    ids_con_nas <- datos_imputados[!complete.cases(datos_imputados$energy_kwh_hh), ]$lc_lid
    for(id in ids_con_nas){
      datos_imputados[!complete.cases(datos_imputados$energy_kwh_hh) & (datos_imputados$lc_lid == id), ]$energy_kwh_hh <- mean(datos_imputados[(datos_imputados$lc_lid == id), ]$energy_kwh_hh, na.rm = TRUE)
    }
  }
  
  # Agregar datos a nivel hora
  datos_imputados <- datos_imputados %>%
    mutate(hora = floor_date(tstp, unit = "hour")) %>%
    mutate(tstp = as.POSIXct(tstp, tz = "Europe/London"))
  
  datos_hourly <- datos_imputados %>%
    group_by(lc_lid, hora) %>%
    summarize(energy_kwh_hh_promedio = mean(energy_kwh_hh, na.rm = TRUE)) %>%
    rename(tstp = hora, energy_kwh_hh = energy_kwh_hh_promedio) %>%
    select(lc_lid, tstp, energy_kwh_hh) %>%
    arrange(tstp)
  
  # Eliminar ID's con + 10% de atípicos
  pct_atipicos_ID <- datos_hourly %>%
    group_by(lc_lid) %>%
    summarize(pct_outliers = sum(is_outlier(energy_kwh_hh)) / n())
  pct_atipicos_ID$eliminar_exceso_atipicos <- ifelse(pct_atipicos_ID$pct_outliers > 0.1, TRUE, FALSE)
  
  datos_hourly <- datos_hourly %>% filter(lc_lid %notin% pct_atipicos_ID[pct_atipicos_ID$eliminar_exceso_atipicos, ]$lc_lid)
  datos_hourly <- left_join(datos_hourly, weather_hourly, by = join_by(tstp == time))
  datos_hourly <- left_join(datos_hourly, informations_households, join_by(lc_lid == LCLid))
  
  datos_hourly$season <- ifelse(month(datos_hourly$tstp) %in% c(3, 4, 5), "Spring", 
                                ifelse(month(datos_hourly$tstp) %in% c(6, 7, 8), "Summer", 
                                       ifelse(month(datos_hourly$tstp) %in% c(9, 10, 11), "Autumn", 
                                              "Winter")))
  
  nombre_archivo_salida <- paste("hourly_clean", basename(archivo), sep = "_")
  write_csv(datos_hourly, file.path(carpeta_salida, nombre_archivo_salida))
}




# Configurar la paralelización
num_cores <- detectCores() - 5  # Detecta el número de núcleos disponibles y usa uno menos para evitar saturar el sistema
cl <- makeCluster(num_cores)

# Exportar las variables y funciones necesarias al clúster
clusterExport(cl, c("archivos", "carpeta_salida", "weather_hourly", "informations_households", 
                    "tstp_faltantes", "imputar_grupo", "is_outlier", "procesar_archivo", "encontrar_tstp_faltantes", 
                    "imputar_grupo", "encontrar_tstp_faltantes_hourly", "tstp_faltantes", "tstp_faltantes_hourly", "is_outlier", "%notin%"))

# Exportar las librerías necesarias al clúster
clusterEvalQ(cl, {
  library(dplyr)
  library(lubridate)
  library(readr)
  library(tidyr)
})

# Ejecutar la función en paralelo
#archivos <- archivos[1:10] #prueba
parLapply(cl, archivos, procesar_archivo, carpeta_salida, weather_hourly, informations_households)

# Detener el clúster
stopCluster(cl)




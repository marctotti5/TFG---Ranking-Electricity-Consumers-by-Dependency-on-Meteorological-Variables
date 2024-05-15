#####################################################################################################################
#####################################################################################################################
# -------- 0.0.1 DETECT HOME (DETECTAR ACTIVIDAD EN LAS CASAS A PARTIR DE LA VARIANZA EN BLOQUES DE 6H) --------------
#####################################################################################################################
#####################################################################################################################

#library(data.table)
#library(dtplyr)
library(dplyr, warn.conflicts = FALSE)
#library(doSNOW) 
#library(foreach)#####################################################################################################################
#####################################################################################################################
# -------- 0.0.1 DETECT HOME (DETECTAR ACTIVIDAD EN LAS CASAS A PARTIR DE LA VARIANZA EN BLOQUES DE 6H) --------------
#####################################################################################################################
#####################################################################################################################

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
library(readr)
library(ggplot2)

## CARGA DE DATOS: de google drive
library(googledrive)
#archivos_drive <- googledrive::drive_find(type = "folder")
#temp <- tempfile(fileext = ".zip")
#dl <- drive_download(as_id("1qtjmDnDh3j6P2aznRp-zzsl8hDC8_AVw"), type = "folder", path = temp, overwrite = TRUE)
#out <- unzip(temp, exdir = tempdir())
#bank <- read.csv(out[14], sep = ";")

#ls_tibble <- googledrive::drive_ls("TFG/hourly_dataset_clean")
#library(googledrive)
#for (file_id in ls_tibble$id) {
#googledrive::drive_download(as_id(file_id))
#}

# Hacer un append de todos los datos hourly en una sola tabla
# Obtener la lista de archivos CSV en la carpeta
#setwd("./Datos/archive/hourly_dataset_clean")
setwd("/home/rstudio-user/Datos/hourly_dataset_clean")

#file_list <- list.files(path = "/hourly_dataset_clean", 
#pattern = "hourly_clean_block_.*\\.csv")
file_list <- list.files()

# Crear una lista para almacenar los datos de los archivos CSV
data_list <- list()



# Iterar sobre cada archivo CSV
for (i in 1:length(file_list)) {
        # Leer el archivo CSV y almacenarlo en la lista de datos
        data_list[[i]] <- read_csv(file_list[i], 
                                   col_types = cols(`energy_kwh_hh` = col_number(),
                                                    lc_lid = col_character()))
}
gc()
# Combinar todos los datos en un solo data frame
datos_hourly <- do.call(rbind, data_list)
rm(data_list)
gc()

# Agrupar los datos por ventana de 6 horas para cada id
datos_hourly_window <- datos_hourly %>%
        mutate(window_start = cut(tstp, breaks = "6 hours")) %>%
        group_by(lc_lid, window_start, season) %>%
        summarize(var_energy = var(energy_kwh_hh, na.rm = TRUE))

# Obtenemos el % de NA's en el cálculo
#sum(!complete.cases(datos_hourly_window))/nrow(datos_hourly_window)*100 # 0.8 % 

## Filtraremos las personas fuera del hogar, utilizando el pct 10 de las varianzas muestrales de CADA estacion
percentil_deseado <- aggregate(var_energy ~ season, data = datos_hourly_window, FUN = function(x) quantile(x, probs = 0.1))
colnames(percentil_deseado)[2] <- "var_energy_pct10_season"

# Filtramos fuera las observaciones en las que la desviacion tipica muestral del bloque de 6h al que pertenecen es menor que el percentil 10%
find_window_block <- function(tstp) {
        return(cut(tstp, breaks = "6 hours"))
}

# Aplicar la función a cada registro en datos_hourly
datos_hourly <- datos_hourly %>%
        mutate(window_start = find_window_block(tstp))

# Unir datos_hourly y datos_hourly_window basándonos en el bloque de 6 horas y el id
datos_hourly <- left_join(datos_hourly, datos_hourly_window, by = c("lc_lid", "window_start", "season"))

# Unir el percentil 10 con datos_hourly basándonos en la estación
datos_hourly <- left_join(datos_hourly, percentil_deseado, by = "season")
#rm(datos_hourly_window)
gc()

# ---- NA's en var_energy ----:
## Comprobamos que estos NA's en var_energy es porque solamente hay un dato por lo que la varianza sale NA (debería salir 0, ya que la varianza de una constante es 0)
#datos_hourly_nas <- datos_hourly[is.na(datos_hourly$var_energy), ]
#sum(nrow(datos_hourly_nas)/nrow(datos_hourly))*100 # 0.138 % -> sustituimos la varianza NA por 0, ya que la varianza de un escalar es 0
#ids_con_na <- datos_hourly[is.na(datos_hourly$var_energy), ]$lc_lid %>% unique()
#lista_comprobacion_nas <- vector(mode = "list", length = nrow(datos_hourly_nas))

#for(i in 1:nrow(datos_hourly_nas)){
#lista_comprobacion_nas[[i]] <- datos_hourly %>% filter(lc_lid == datos_hourly_nas[i, ]$lc_lid & 
#date(tstp) == date(datos_hourly_nas[i, ]$tstp)) %>%
#select(lc_lid, tstp, window_start, energy_kwh_hh, var_energy)
#}

# Vemos que en todos estos casos los NA's se dan porque no hay suficientes datos para calcular una varianza (un solo valor)
# entonces el resultado de la varianza debería ser 0, pero es NA
#print(lista_comprobacion_nas) 

# Sustituimos los NA's por 0 (son NA's porque la varianza de un solo número da NA)
datos_hourly[is.na(datos_hourly$var_energy), "var_energy"] <- 0
gc()

## ---- IS_HOME ------- Crear la columna is_home basándose en la condición especificada para cada estación
information_household <- read_csv("/home/rstudio-user/data/informations_households.csv")
datos_hourly <- datos_hourly %>%
        mutate(is_home = var_energy >= var_energy_pct10_season) %>% 
        select(lc_lid, tstp, energy_kwh_hh, is_home, everything(), window_start, 
               -var_energy_pct10_season, -var_energy, -window_start) %>%
        left_join(information_household, by = join_by(lc_lid == LCLid), keep = FALSE)
gc()
drop.cols <- c("stdorToU.y", "Acorn.y", "Acorn_grouped.y", "file.y")

nuevos_nombres_columnas <- c(stdorToU = "stdorToU.x", Acorn = "Acorn.x", 
                             Acorn_grouped = "Acorn_grouped.x", file = "file.x")
datos_hourly <- datos_hourly %>% 
        select(-one_of(drop.cols)) %>% 
        rename(all_of(nuevos_nombres_columnas))
gc()

#drive_upload(datos_hourly, paste0("TFG/hourly_dataset_clean_V3_SI_ISHOME/", "datos_hourly_full_clean_ishome", ".csv"))
#write_csv(datos_hourly, "/home/rstudio-user/data/datos_hourly_clean_ishome_full.csv")


# Ahora volvemos a dividir en 112 csvs por bloque y guardamos los csvs
setwd("/home/rstudio-user/data/hourly_dataset_clean_ISHOME")
datos_hourly_grouped <- datos_hourly %>% group_by(file)
datos_hourly_grouped <- group_split(datos_hourly_grouped)
for(i in 1:length(datos_hourly_grouped)){
        write_csv(datos_hourly_grouped[[i]], paste0("hourly_clean_def_block_", i-1, ".csv"))
        #datos_hourly_grouped[[i]] <- NULL
}
rm(datos_hourly)
rm(information_household)
rm(percentil_deseado)
gc()

# Exportamos los csvs generados a la carpeta en drive
file_list <- list.files()
for(i in 1:length(file_list)){
        drive_upload(file_list[[i]], paste0("TFG/hourly_dataset_clean_V3_SI_ISHOME/", file_list[[i]], ".csv"))
}















# 3. VENTANAS DE VARIABILIDAD: DETECTAR INDIVIDUOS FUERA DE CASA:
## Estimamos la distribución de la varianza del consumo en bloques de 6h
## y vemos a partir de qué treshold consideramos que el cliente no está en casa

# Obtenemos el % de NA's en el cálculo
sum(!complete.cases(datos_hourly_window))/nrow(datos_hourly_window)*100 # 0.66 % -> los eliminamos
datos_hourly_window[is.na(datos_hourly_window$var_energy), "var_energy"] <- 0 # son NA's inducidos pq la varianza de un número solo es NA       

# Ver la distribución estimada de las varianzas
## Distribución genérica
histogram_var_window <- ggplot(data = datos_hourly_window, aes(x = var_energy)) + 
        geom_histogram(color = "black", fill = "lightblue", binwidth = 0.01) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas",
             x = "Varianza muestral de Consumo de Energía (kWh)",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "top") + xlim(c(0, 1)) + ylim(c(0, 2.5*10^6))

histogram_var_window_zoom_10 <- ggplot(data = datos_hourly_window, aes(x = var_energy)) + 
        geom_histogram(color = "black", fill = "lightblue", binwidth = 0.001) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas (ZOOM x 10)",
             x = "Varianza muestral de Consumo de Energía (kWh)",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "top") + xlim(c(0, 0.1)) + ylim(c(0, 2.1*10^6))

## Distribución por estación: filtraremos las personas fuera del hogar, utilizando el pct 10 de las varianzas muestrales de CADA estacion
percentil_deseado <- aggregate(var_energy ~ season, data = datos_hourly_window, FUN = function(x) quantile(x, probs = 0.1))
histogram_var_window_zoom_100_season <- ggplot(data = datos_hourly_window, aes(x = var_energy, fill = season)) + 
        geom_histogram(color = "black", binwidth = 0.00001) + xlim(c(0, 0.001)) +
        #scale_fill_manual(values = c("Winter" = "purple", "Autumn" = "brown1", "Summer" = "darkturquoise", "Spring" = "chartreuse4")) +
        facet_wrap(~ season) +
        geom_vline(data = percentil_deseado, aes(xintercept = var_energy, color = season), linetype = "dashed") +
        #geom_vline(aes(xintercept = quantile(var_energy, probs = 0.1, na.rm = T)), 
        #color = "red", linetype = "dashed") +
        # Graficamos el percentil 10% de las varianzas muestrales
        # Supondremos que si un hogar se situa en una varianza muestral menor al pct 10% durante x horas, esas horas se considera q está fuera de casa
        geom_text(data = percentil_deseado, aes(x = var_energy, 
                                                y = 75000 + 0.1, label = paste("Pct 10:", round(var_energy, 8)), 
                                                color = season), vjust = 1, hjust = -0.05) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas y por estación (ZOOM x 100)",
             x = "Varianza muestral de Consumo de Energía",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "none")

# Calcular estadísticas resumidas de la distribución
# GRAFICAR SERIE DE VARIANZAS AGREGADA POR HORAS, Y VER EN QUÉ HORAS HAY MENOS VARIABILIDAD -> MENOS GENTE EN CASA
datos_hourly_grouped_hour <- datos_hourly_window %>% 
        group_by(hora = hour(window_start)) %>% 
        summarise(var_energy = mean(var_energy, na.rm = TRUE))

## Serie temporal de la media de las varianzas en bloques de 6h
datos_hourly_window_grouped <- datos_hourly_window %>% group_by(window_start) %>% 
        summarise(var_energy_avg = mean(var_energy, na.rm = T))
datos_hourly_window_grouped$window_start <- as.POSIXct(datos_hourly_window_grouped$window_start, 
                                                       format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London")
datos_hourly_window_grouped$season <- ifelse(month(datos_hourly_window_grouped$window_start) %in% c(3, 4, 5), "Spring", 
                                             ifelse(month(datos_hourly_window_grouped$window_start) %in% c(6, 7, 8), "Summer", 
                                                    ifelse(month(datos_hourly_window_grouped$window_start) %in% c(9, 10, 11), "Autumn", 
                                                           "Winter")))

serie_var_avg_energy <- ggplot(datos_hourly_window_grouped, aes(x = window_start)) +
        geom_line(aes(y = var_energy_avg)) +
        scale_y_continuous(
                name = "Varianzas Promedio de Energía Consumida en Bloques de 6h",
        )+
        labs(title = "Serie de Varianzas Promedio de Energía Consumida en Bloques de 6h", 
             x = "Fecha")










# Probar con bootstrap para series temporales (block bootstrap, articulo guardado en zotero)
"https://drive.google.com/file/d/1m6Rh5rNHvKLijFY93t_zYzNrcIesaafo/view?usp=drive_link"

#library(parallel)
#library(doParallel)
#library(sparklyr)
#library(tidyverse)
library(lubridate)
library(readr)
library(tidyr)
#library(mice)
#library(xts)
library(readr)
library(ggplot2)

## CARGA DE DATOS: de google drive
library(googledrive)
#archivos_drive <- googledrive::drive_find(type = "folder")
#temp <- tempfile(fileext = ".zip")
#dl <- drive_download(as_id("1qtjmDnDh3j6P2aznRp-zzsl8hDC8_AVw"), type = "folder", path = temp, overwrite = TRUE)
#out <- unzip(temp, exdir = tempdir())
#bank <- read.csv(out[14], sep = ";")

#ls_tibble <- googledrive::drive_ls("TFG/hourly_dataset_clean")
#library(googledrive)
#for (file_id in ls_tibble$id) {
#googledrive::drive_download(as_id(file_id))
#}

# Hacer un append de todos los datos hourly en una sola tabla
# Obtener la lista de archivos CSV en la carpeta
#setwd("./Datos/archive/hourly_dataset_clean")
setwd("/home/rstudio-user/Datos/hourly_dataset_clean")

#file_list <- list.files(path = "/hourly_dataset_clean", 
#pattern = "hourly_clean_block_.*\\.csv")
file_list <- list.files()

# Crear una lista para almacenar los datos de los archivos CSV
data_list <- list()



# Iterar sobre cada archivo CSV
for (i in 1:length(file_list)) {
        # Leer el archivo CSV y almacenarlo en la lista de datos
        data_list[[i]] <- read_csv(file_list[i], 
                                   col_types = cols(`energy_kwh_hh` = col_number(),
                                                    lc_lid = col_character()))
}
gc()
# Combinar todos los datos en un solo data frame
datos_hourly <- do.call(rbind, data_list)
rm(data_list)
gc()

# Agrupar los datos por ventana de 6 horas para cada id
datos_hourly_window <- datos_hourly %>%
        mutate(window_start = cut(tstp, breaks = "6 hours")) %>%
        group_by(lc_lid, window_start, season) %>%
        summarize(var_energy = var(energy_kwh_hh, na.rm = TRUE))

# Obtenemos el % de NA's en el cálculo
#sum(!complete.cases(datos_hourly_window))/nrow(datos_hourly_window)*100 # 0.8 % 

## Filtraremos las personas fuera del hogar, utilizando el pct 10 de las varianzas muestrales de CADA estacion
percentil_deseado <- aggregate(var_energy ~ season, data = datos_hourly_window, FUN = function(x) quantile(x, probs = 0.1))
colnames(percentil_deseado)[2] <- "var_energy_pct10_season"

# Filtramos fuera las observaciones en las que la desviacion tipica muestral del bloque de 6h al que pertenecen es menor que el percentil 10%
find_window_block <- function(tstp) {
        return(cut(tstp, breaks = "6 hours"))
}

# Aplicar la función a cada registro en datos_hourly
datos_hourly <- datos_hourly %>%
        mutate(window_start = find_window_block(tstp))

# Unir datos_hourly y datos_hourly_window basándonos en el bloque de 6 horas y el id
datos_hourly <- left_join(datos_hourly, datos_hourly_window, by = c("lc_lid", "window_start", "season"))

# Unir el percentil 10 con datos_hourly basándonos en la estación
datos_hourly <- left_join(datos_hourly, percentil_deseado, by = "season")
#rm(datos_hourly_window)
gc()

# ---- NA's en var_energy ----:
## Comprobamos que estos NA's en var_energy es porque solamente hay un dato por lo que la varianza sale NA (debería salir 0, ya que la varianza de una constante es 0)
#datos_hourly_nas <- datos_hourly[is.na(datos_hourly$var_energy), ]
#sum(nrow(datos_hourly_nas)/nrow(datos_hourly))*100 # 0.138 % -> sustituimos la varianza NA por 0, ya que la varianza de un escalar es 0
#ids_con_na <- datos_hourly[is.na(datos_hourly$var_energy), ]$lc_lid %>% unique()
#lista_comprobacion_nas <- vector(mode = "list", length = nrow(datos_hourly_nas))

#for(i in 1:nrow(datos_hourly_nas)){
#lista_comprobacion_nas[[i]] <- datos_hourly %>% filter(lc_lid == datos_hourly_nas[i, ]$lc_lid & 
#date(tstp) == date(datos_hourly_nas[i, ]$tstp)) %>%
#select(lc_lid, tstp, window_start, energy_kwh_hh, var_energy)
#}

# Vemos que en todos estos casos los NA's se dan porque no hay suficientes datos para calcular una varianza (un solo valor)
# entonces el resultado de la varianza debería ser 0, pero es NA
#print(lista_comprobacion_nas) 

# Sustituimos los NA's por 0 (son NA's porque la varianza de un solo número da NA)
datos_hourly[is.na(datos_hourly$var_energy), "var_energy"] <- 0
gc()

## ---- IS_HOME ------- Crear la columna is_home basándose en la condición especificada para cada estación
information_household <- read_csv("/home/rstudio-user/data/informations_households.csv")
datos_hourly <- datos_hourly %>%
        mutate(is_home = var_energy >= var_energy_pct10_season) %>% 
        select(lc_lid, tstp, energy_kwh_hh, is_home, everything(), window_start, 
               -var_energy_pct10_season, -var_energy, -window_start) %>%
        left_join(information_household, by = join_by(lc_lid == LCLid), keep = FALSE)
gc()
drop.cols <- c("stdorToU.y", "Acorn.y", "Acorn_grouped.y", "file.y")

nuevos_nombres_columnas <- c(stdorToU = "stdorToU.x", Acorn = "Acorn.x", 
                             Acorn_grouped = "Acorn_grouped.x", file = "file.x")
datos_hourly <- datos_hourly %>% 
        select(-one_of(drop.cols)) %>% 
        rename(all_of(nuevos_nombres_columnas))
gc()

#drive_upload(datos_hourly, paste0("TFG/hourly_dataset_clean_V3_SI_ISHOME/", "datos_hourly_full_clean_ishome", ".csv"))
#write_csv(datos_hourly, "/home/rstudio-user/data/datos_hourly_clean_ishome_full.csv")


# Ahora volvemos a dividir en 112 csvs por bloque y guardamos los csvs
setwd("/home/rstudio-user/data/hourly_dataset_clean_ISHOME")
datos_hourly_grouped <- datos_hourly %>% group_by(file)
datos_hourly_grouped <- group_split(datos_hourly_grouped)
for(i in 1:length(datos_hourly_grouped)){
        write_csv(datos_hourly_grouped[[i]], paste0("hourly_clean_def_block_", i, ".csv"))
        #datos_hourly_grouped[[i]] <- NULL
}
rm(datos_hourly)
rm(information_household)
rm(percentil_deseado)
gc()

# Exportamos los csvs generados a la carpeta en drive
file_list <- list.files()
for(i in 1:length(file_list)){
        drive_upload(file_list[[i]], paste0("TFG/hourly_dataset_clean_V3_SI_ISHOME/", file_list[[i]], ".csv"))
}















# 3. VENTANAS DE VARIABILIDAD: DETECTAR INDIVIDUOS FUERA DE CASA:
## Estimamos la distribución de la varianza del consumo en bloques de 6h
## y vemos a partir de qué treshold consideramos que el cliente no está en casa

# Obtenemos el % de NA's en el cálculo
sum(!complete.cases(datos_hourly_window))/nrow(datos_hourly_window)*100 # 0.66 % -> los eliminamos
datos_hourly_window[is.na(datos_hourly_window$var_energy), "var_energy"] <- 0 # son NA's inducidos pq la varianza de un número solo es NA       

# Ver la distribución estimada de las varianzas
## Distribución genérica
histogram_var_window <- ggplot(data = datos_hourly_window, aes(x = var_energy)) + 
        geom_histogram(color = "black", fill = "lightblue", binwidth = 0.01) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas",
             x = "Varianza muestral de Consumo de Energía (kWh)",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "top") + xlim(c(0, 1)) + ylim(c(0, 2.5*10^6))

histogram_var_window_zoom_10 <- ggplot(data = datos_hourly_window, aes(x = var_energy)) + 
        geom_histogram(color = "black", fill = "lightblue", binwidth = 0.001) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas (ZOOM x 10)",
             x = "Varianza muestral de Consumo de Energía (kWh)",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "top") + xlim(c(0, 0.1)) + ylim(c(0, 2.5*10^6))

## Distribución por estación: filtraremos las personas fuera del hogar, utilizando el pct 10 de las varianzas muestrales de CADA estacion
percentil_deseado <- aggregate(var_energy ~ season, data = datos_hourly_window, FUN = function(x) quantile(x, probs = 0.1))
histogram_var_window_zoom_100_season <- ggplot(data = datos_hourly_window, aes(x = var_energy, fill = season)) + 
        geom_histogram(color = "black", binwidth = 0.00001) + xlim(c(0, 0.001)) +
        #scale_fill_manual(values = c("Winter" = "purple", "Autumn" = "brown1", "Summer" = "darkturquoise", "Spring" = "chartreuse4")) +
        facet_wrap(~ season) +
        geom_vline(data = percentil_deseado, aes(xintercept = var_energy, color = season), linetype = "dashed") +
        #geom_vline(aes(xintercept = quantile(var_energy, probs = 0.1, na.rm = T)), 
        #color = "red", linetype = "dashed") +
        # Graficamos el percentil 10% de las varianzas muestrales
        # Supondremos que si un hogar se situa en una varianza muestral menor al pct 10% durante x horas, esas horas se considera q está fuera de casa
        geom_text(data = percentil_deseado, aes(x = var_energy, 
                                                y = 75000 + 0.1, label = paste("Pct 10:", round(var_energy, 8)), 
                                                color = season), vjust = 1, hjust = -0.05) +
        labs(title = "Distribución de Varianzas muestrales del Consumo Energético",
             subtitle = "en ventanas de 6 horas y por estación (ZOOM x 100)",
             x = "Varianza muestral de Consumo de Energía",
             y = "Frecuencia Absoluta") +
        theme(legend.position = "none")

# Calcular estadísticas resumidas de la distribución
# GRAFICAR SERIE DE VARIANZAS AGREGADA POR HORAS, Y VER EN QUÉ HORAS HAY MENOS VARIABILIDAD -> MENOS GENTE EN CASA
datos_hourly_grouped_hour <- datos_hourly_window %>% 
        group_by(hora = hour(window_start)) %>% 
        summarise(var_energy = mean(var_energy, na.rm = TRUE))

## Serie temporal de la media de las varianzas en bloques de 6h
datos_hourly_window_grouped <- datos_hourly_window %>% group_by(window_start) %>% 
        summarise(var_energy_avg = mean(var_energy, na.rm = T))
datos_hourly_window_grouped$window_start <- as.POSIXct(datos_hourly_window_grouped$window_start, 
                                                       format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London")
datos_hourly_window_grouped$season <- ifelse(month(datos_hourly_window_grouped$window_start) %in% c(3, 4, 5), "Spring", 
                                             ifelse(month(datos_hourly_window_grouped$window_start) %in% c(6, 7, 8), "Summer", 
                                                    ifelse(month(datos_hourly_window_grouped$window_start) %in% c(9, 10, 11), "Autumn", 
                                                           "Winter")))

serie_var_avg_energy <- ggplot(datos_hourly_window_grouped, aes(x = window_start)) +
        geom_line(aes(y = var_energy_avg)) +
        scale_y_continuous(
                name = "Varianzas Promedio de Energía Consumida en Bloques de 6h",
        )+
        labs(title = "Serie de Varianzas Promedio de Energía Consumida en Bloques de 6h", 
             x = "Fecha")










# Probar con bootstrap para series temporales (block bootstrap, articulo guardado en zotero)
"https://drive.google.com/file/d/1m6Rh5rNHvKLijFY93t_zYzNrcIesaafo/view?usp=drive_link"

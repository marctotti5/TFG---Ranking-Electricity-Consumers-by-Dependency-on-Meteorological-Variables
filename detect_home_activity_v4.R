#####################################################################################################################
#####################################################################################################################
# -------- 0.0.1 DETECT HOME (DETECTAR ACTIVIDAD EN LAS CASAS A PARTIR DE LA VARIANZA EN BLOQUES DE 6H) --------------
#####################################################################################################################
#####################################################################################################################


library(dplyr, warn.conflicts = FALSE)
#library(tidyverse)
library(lubridate)
library(readr)
library(tidyr)
library(readr)
library(ggplot2)

## CARGA DE DATOS: de google drive
#library(googledrive)
#ls_tibble <- googledrive::drive_ls("TFG/hourly_dataset_clean")
#library(googledrive)
#for (file_id in ls_tibble$id) {
#googledrive::drive_download(as_id(file_id))
#}

## LECTURA DE DATOS
# Obtener la lista de archivos CSV en la carpeta
#setwd("./Datos/archive/hourly_dataset_clean")
setwd("/home/rstudio-user/Datos/hourly_dataset_clean")
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

# Estandarizar la energía y calcular las varianzas sobre las energias estandarizadas
datos_hourly <- datos_hourly %>%
        group_by(lc_lid) %>%
        mutate(mean_energy = mean(energy_kwh_hh),
               sd_energy = sd(energy_kwh_hh)) %>%
        ungroup()
datos_hourly <- datos_hourly %>%
        mutate(energy_standardized = (energy_kwh_hh - mean_energy) / sd_energy)
datos_hourly$mean_energy <- NULL
datos_hourly$sd_energy <- NULL




# Agrupar los datos por ventana de 6 horas para cada id
datos_hourly_window <- datos_hourly %>%
        mutate(window_start = cut(tstp, breaks = "6 hours")) %>%
        group_by(lc_lid, window_start, season) %>%
        summarize(var_energy_standarized = var(energy_standardized, na.rm = TRUE))


# Obtenemos el % de NA's en el cálculo
sum(!complete.cases(datos_hourly_window))/nrow(datos_hourly_window)*100 # 0.8 % -> los eliminamos
datos_hourly_window[is.na(datos_hourly_window$var_energy), "var_energy"] <- 0


# Exportamos datos_hourly_window
write_csv(datos_hourly_window, "TFG/tfg_marc/TFG---Marc-Pastor/datos_hourly_window.csv")


## Filtraremos las personas fuera del hogar, utilizando el pct 10 de las varianzas muestrales de CADA estacion
percentil_deseado <- aggregate(var_energy_standarized ~ lc_lid + season, data = datos_hourly_window, FUN = function(x) quantile(x, probs = 0.1))
colnames(percentil_deseado)[2] <- "var_energy_standarized_pct10_season"

# Filtramos fuera las observaciones en las que la varianza muestral del bloque de 6h al que pertenecen es menor que el percentil 10%
find_window_block <- function(tstp) {
        return(cut(tstp, breaks = "6 hours"))
}

# Aplicar la función a cada registro en datos_hourly
datos_hourly <- datos_hourly %>%
        mutate(window_start = find_window_block(tstp))

# Unir datos_hourly y datos_hourly_window basándonos en el bloque de 6 horas y el id
datos_hourly <- left_join(datos_hourly, datos_hourly_window, by = c("lc_lid", "window_start", "season"))

# Unir el percentil 10 con datos_hourly basándonos en la estación
datos_hourly <- left_join(datos_hourly, percentil_deseado, by = c("lc_lid", "season"))
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
datos_hourly[is.na(datos_hourly$var_energy_standarized), "var_energy_standarized"] <- 0
gc()

## ---- IS_HOME ------- Crear la columna is_home basándose en la condición especificada para cada estación
#information_household <- read_csv("/home/rstudio-user/data/informations_households.csv")
datos_hourly <- datos_hourly %>%
        mutate(is_home = var_energy_standarized >= var_energy_standarized_pct10_season) %>% 
        select(lc_lid, tstp, energy_kwh_hh, is_home, everything(), -var_energy_pct10_season, 
               -var_energy_standarized, -window_start, -energy_standardized) 
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

## OJO: CORREGIR NOMBRE DE ARCHIVO!!
carpeta_entrada <- "./Datos/archive/hourly_dataset_clean_ISHOME"
carpeta_salida <- "./Datos/archive/hourly_dataset_clean_ISHOME_corrected"
file_list <- list.files(carpeta_entrada, full.names = T)
for(i in 1:length(file_list)){
        datos_hourly <- read_csv(file_list[i], 
                                 col_types = cols(`energy_kwh_hh` = col_number(),
                                                  lc_lid = col_character()))
        write_csv(datos_hourly, paste0(carpeta_salida, "/hourly_clean_def_", unique(datos_hourly$file), ".csv"))
        #datos_hourly_grouped[[i]] <- NULL
}


# Exportamos los csvs generados a la carpeta en drive
file_list <- list.files()
for(i in 1:length(file_list)){
        drive_upload(file_list[[i]], paste0("TFG/hourly_dataset_clean_V3_SI_ISHOME/", file_list[[i]]$file, ".csv"))
}




# Exportamos el archivo entero datos_hourly
write_csv(datos_hourly_window, "TFG/tfg_marc/TFG---Marc-Pastor/datos_hourly_clean_ishome_full.csv")




## CORRECCIÓN NUMEROS DE DOCUMENTO:
carpeta_entrada <- "./Datos/archive/hourly_dataset_clean_ISHOME"
carpeta_salida <- "./Datos/archive/hourly_dataset_clean_ISHOME_corrected"
file_list <- list.files(carpeta_entrada, full.names = T)
for(i in 1:length(file_list)){
        datos_hourly <- read_csv(file_list[i], 
                                 col_types = cols(`energy_kwh_hh` = col_number(),
                                                  lc_lid = col_character()))
        write_csv(datos_hourly, paste0(carpeta_salida, "/hourly_clean_def_", unique(datos_hourly$file), ".csv"))
        #datos_hourly_grouped[[i]] <- NULL
}

## CORRECCIÓN NUMEROS DE DOCUMENTO (ishome_stationary)
carpeta_entrada <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary"
carpeta_salida <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary_corrected"
file_list <- list.files(carpeta_entrada, full.names = T)
for(i in 1:length(file_list)){
        datos_hourly <- read_csv(file_list[i], 
                                 col_types = cols(`energy_kwh_hh` = col_number(),
                                                  lc_lid = col_character()))
        write_csv(datos_hourly, paste0(carpeta_salida, "/hourly_clean_def_", unique(datos_hourly$file), "_stationary.csv"))
        #datos_hourly_grouped[[i]] <- NULL
}





















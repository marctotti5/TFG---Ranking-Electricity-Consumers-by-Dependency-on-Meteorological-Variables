#########################################################################################################
###################################### -- CÁLCULO DEL GCC -- ############################################
#########################################################################################################

## CARGA DE LIBRERÍAS
library(dplyr, warn.conflicts = FALSE)
library(lubridate)
library(readr)
library(tidyr)
library(ggplot2)
library(xts)
library(DataExplorer)
library(ggforce)
library(SLBDD) #GCCMatrix()
library(forecast)
# para procesamiento en paralelo
library(purrr)
library(foreach)
library(doParallel)
#library(qfa)
library(proxy)
library(fastcluster)
library(factoextra)
library(dendextend)
library(cluster)
library(cluster)
library(NbClust)
library(janitor)
library(rcompanion)

## ----------------------------------- PREPROCESAMIENTO DE LAS SERIES -------------------------------------
### ----------------------------------- TRATAMIENTO DE DATOS ATÍPICOS -------------------------------------
### ------------------------ ESTACIONALIZACIÓN Y ESTANDARIZACIÓN DE LAS SERIES ----------------------------
# Realizamos la descomposición MSTL de la serie de energías y de la variable meteorológica de interés
# Restamos las componentes estimadas de la serie original, obteniendo así la serie estacional
# Estandarizamos a media y varianza unidad:

# Cargamos los datos
datos_hourly <- read_csv("./Datos/archive/hourly_dataset_clean_ISHOME/hourly_clean_def_block_107.csv", 
                         col_types = cols(`energy_kwh_hh` = col_number(),
                                          lc_lid = col_character()))


## -----------------------------------------------------------------------------------
## ---------------------------- FUNCION PREPROCESAMIENTO -----------------------------
## -----------------------------------------------------------------------------------
# Función para detectar y sustituir valores atípicos, desestacionalizar y estandarizar las variables numéricas de un dataframe
preprocess_GCC_old <- function(df, transformation = "mstl", impute_outliers = TRUE, standarize = FALSE) {
    
    # Detectar la columna de datetime
    datetime_col <- sapply(df, function(x) inherits(x, "POSIXt"))
    datetime_col <- names(df)[datetime_col]
    
    if (length(datetime_col) != 1) {
        stop("El dataframe debe contener una y solo una columna de tipo datetime")
    }
    
    # Detectar la fecha mínima
    datetime_min <- min(df$tstp)
    
    # Crear columnas adicionales para hora, semana y mes
    df <- df %>%
        mutate(hour = hour(tstp),
               week = week(tstp),
               month = month(tstp),
               year = year(tstp))
    
    # Identificar columnas numéricas
    numeric_cols <- sapply(df, is.numeric)
    numeric_cols <- names(df)[numeric_cols]
    
    # Excluir columnas adicionales creadas
    numeric_cols <- setdiff(numeric_cols, c("hour", "week", "month", "year"))
    
    # Función interna para procesar cada columna
    process_column <- function(series, datetime_series, is_home) {
        # Ahora mismo la lluvia da muchos atípicos
        Q1 = quantile(series, 0.25, na.rm = TRUE)
        Q3 = quantile(series, 0.75, na.rm = TRUE)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        pct_atipicos_pre = sum(series < lower_bound | series > upper_bound)/length(series)
        
        # Crear un data frame temporal
        temp_df <- data.frame(datetime = datetime_series, numeric_variable = series, is_home = is_home) %>% 
            mutate(hour = hour(datetime),
                   week = week(datetime),
                   month = month(datetime),
                   year = year(datetime),
                   # Elimino los usuarios que no están en casa  (imputo NA's)
                   numeric_variable = ifelse(is_home == FALSE, NA, numeric_variable))
        
        # Estacionarizar la serie antes de detectar atípicos
        
        
        
        
        # Imputación de atípicos
        if(impute_outliers == TRUE){
            
            # Primera capa de imputación: por hora dentro de la misma semana
            temp_df <- temp_df %>%
                group_by(week, hour) %>%
                mutate(
                    Q1 = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3 = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR = Q3 - Q1,
                    lower_bound = Q1 - 1.5 * IQR,
                    upper_bound = Q3 + 1.5 * IQR,
                    is_outlier = numeric_variable < lower_bound | numeric_variable > upper_bound,
                    numeric_variable = ifelse(is_outlier, median(numeric_variable[!is_outlier], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
            
            # Segunda capa de imputación: por hora dentro del mismo mes
            temp_df <- temp_df %>%
                group_by(month, hour) %>%
                mutate(
                    Q1_month = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3_month = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR_month = Q3_month - Q1_month,
                    lower_bound_month = Q1_month - 1.5 * IQR_month,
                    upper_bound_month = Q3_month + 1.5 * IQR_month,
                    is_outlier_month = numeric_variable < lower_bound_month | numeric_variable > upper_bound_month,
                    numeric_variable = ifelse(is_outlier_month, median(numeric_variable[!is_outlier_month], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
            
            # Tercera capa de imputación: por hora dentro del mismo año
            temp_df <- temp_df %>%
                group_by(year, hour) %>%
                mutate(
                    Q1_year = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3_year = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR_year = Q3_year - Q1_year,
                    lower_bound_year = Q1_year - 1.5 * IQR_year,
                    upper_bound_year = Q3_year + 1.5 * IQR_year,
                    is_outlier_year = numeric_variable < lower_bound_year | numeric_variable > upper_bound_year,
                    numeric_variable = ifelse(is_outlier_year, median(numeric_variable[!is_outlier_year], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
            
            # Cuarta capa imputación: atípicos generales
            temp_df <- temp_df %>%
                mutate(
                    Q1_global = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3_global = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR_global = Q3_global - Q1_global,
                    lower_bound_global = Q1_global - 1.5 * IQR_global,
                    upper_bound_global = Q3_global + 1.5 * IQR_global,
                    is_outlier_global = numeric_variable < lower_bound_global | numeric_variable > upper_bound_global,
                    numeric_variable = ifelse(is_outlier_global, median(numeric_variable[!is_outlier_global], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
        } 
        
        
        # DESCOMPOSICION MSTL DE LA VARIABLE CON DATOS (IMPUTADOS si asi se ha seleccionado)
        # Convertir la serie en un objeto ts para aplicar MSTL
        # Si se trata de la variable energía, los periodos estacionales son diarios y semanales
        # Si se trata de una variable meteorológica, los periodos serán diarios
        if(transformation == "mstl"){
            if(col == "energy_kwh_hh"){
                seasonal_periods <- c(24, 24*7)
                numeric_variable_ts <- msts(log(temp_df$numeric_variable), seasonal.periods = seasonal_periods, 
                                            start = c(year(min(temp_df$datetime)), 
                                                      as.numeric(format(min(temp_df$datetime), "%j")) + 
                                                          hour(min(temp_df$datetime))/24))
            } else {
                seasonal_periods <- c(24)
                numeric_variable_ts <- msts(temp_df$numeric_variable, seasonal.periods = seasonal_periods, 
                                            start = c(year(min(temp_df$datetime)), 
                                                      as.numeric(format(min(temp_df$datetime), "%j")) + 
                                                          hour(min(temp_df$datetime))/24))
            }
            
            # Aplicar MSTL para descomponer la serie
            mstl_decomposition <- mstl(numeric_variable_ts)
            remainder <- mstl_decomposition[, "Remainder"]
            numeric_variable_stationary <- remainder
        } else if(transformation == "seasonal_difference"){
            numeric_variable_stationary <- temp_df$numeric_variable - lag(temp_df$numeric_variable, 24)
        }
        
        
        # Estandarización a media 0 y varianza 1
        if(standarize == TRUE){
            temp_df$numeric_variable_standardized <- (numeric_variable_stationary - mean(numeric_variable_stationary, na.rm = T))/sd(numeric_variable_stationary, na.rm = T)
            return(temp_df$numeric_variable_standardized)
        } else {
            return(numeric_variable_stationary)
        }
        
    }
    
    # Aplicar la función interna a todas las columnas numéricas
    #vector_atipicos <- vector(mode = "numeric", length = length(numeric_cols))
    #names(vector_atipicos) <- numeric_cols
    for (col in numeric_cols) {
        df[[col]] <- as.vector(process_column(df[[col]], df$tstp, df$is_home))
        #IQR_global = IQR(df[[col]], na.rm = TRUE)
        #lower_bound_global = quantile(df[[col]], 0.25, na.rm = T) - 1.5 * IQR_global
        #upper_bound_global = quantile(df[[col]], 0.75, na.rm = T) + 1.5 * IQR_global
        #is_outlier_prop = sum(df[[col]] < lower_bound_global | df[[col]] > upper_bound_global, na.rm = T)/nrow(df)
        #vector_atipicos[names(vector_atipicos) == col] <- is_outlier_prop
    }
    
    df_preprocessed <- df %>% select(everything(), -hour, -week, -month, -year, -is_home)
    return(df_preprocessed)
    # Devolver lista con df imputado y vector de porcentaje de atípicos por columna una vez estandarizado
    #lista_resultados <- list(df_preprocessed, vector_atipicos)
    #names(lista_resultados) <- c("df_preprocessed", "proporcion_atipicos")
    #return(lista_resultados)
    
}
preprocess_GCC <- function(df, transformation = "mstl", impute_outliers = TRUE, standarize = FALSE) {
    
    # Detectar la columna de datetime
    datetime_col <- sapply(df, function(x) inherits(x, "POSIXt"))
    datetime_col <- names(df)[datetime_col]
    
    if (length(datetime_col) != 1) {
        stop("El dataframe debe contener una y solo una columna de tipo datetime")
    }
    
    # Detectar la fecha mínima
    datetime_min <- min(df$tstp)
    
    # Crear columnas adicionales para hora, semana y mes
    df <- df %>%
        mutate(hour = hour(tstp),
               week = week(tstp),
               month = month(tstp),
               year = year(tstp))
    
    # Identificar columnas numéricas
    numeric_cols <- sapply(df, is.numeric)
    numeric_cols <- names(df)[numeric_cols]
    
    # Excluir columnas adicionales creadas
    numeric_cols <- setdiff(numeric_cols, c("hour", "week", "month", "year"))
    
    # Función interna para procesar cada columna
    process_column <- function(series, datetime_series, is_home) {
        # Crear un data frame temporal
        temp_df <- data.frame(datetime = datetime_series, numeric_variable = series, is_home = is_home) %>% 
            mutate(hour = hour(datetime),
                   week = week(datetime),
                   month = month(datetime),
                   quarter = quarter(datetime),
                   year = year(datetime))
        #numeric_variable = ifelse(is_home == FALSE, NA, numeric_variable))
        
        # Estacionarizar la serie antes de detectar atípicos
        # DESCOMPOSICION MSTL DE LA VARIABLE CON DATOS
        # Convertir la serie en un objeto ts para aplicar MSTL
        # Si se trata de la variable energía, los periodos estacionales son diarios y semanales
        # Si se trata de una variable meteorológica, los periodos serán diarios
        if(transformation == "mstl"){
            if(col == "energy_kwh_hh"){
                seasonal_periods <- c(24, 24*7)
                numeric_variable_ts <- msts(log(1 + temp_df$numeric_variable), seasonal.periods = seasonal_periods, 
                                            start = c(year(min(temp_df$datetime)), 
                                                      as.numeric(format(min(temp_df$datetime), "%j")) + 
                                                          hour(min(temp_df$datetime))/24))
            } else {
                seasonal_periods <- c(24)
                numeric_variable_ts <- msts(temp_df$numeric_variable, seasonal.periods = seasonal_periods, 
                                            start = c(year(min(temp_df$datetime)), 
                                                      as.numeric(format(min(temp_df$datetime), "%j")) + 
                                                          hour(min(temp_df$datetime))/24))
            }
            
            # Aplicar MSTL para descomponer la serie
            mstl_decomposition <- mstl(numeric_variable_ts)
            remainder <- mstl_decomposition[, "Remainder"]
            numeric_variable_stationary <- remainder
        } else if(transformation == "seasonal_difference"){
            numeric_variable_stationary <- temp_df$numeric_variable - lag(temp_df$numeric_variable, 24)
        }
        
        # Sustituimos la variable estacionaria
        temp_df$numeric_variable <- numeric_variable_stationary
        
        # Eliminamos aquellos valores donde el usuario no está en casa
        temp_df$numeric_variable <- ifelse(temp_df$is_home == FALSE, NA, temp_df$numeric_variable)
        
        
        # Imputación de atípicos
        if(impute_outliers == TRUE){
            
            # Primera capa de imputación: por hora dentro del mismo trimestre
            temp_df <- temp_df %>%
                group_by(quarter, hour) %>%
                mutate(
                    Q1_quarter = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3_quarter = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR_quarter = Q3_quarter - Q1_quarter,
                    lower_bound_quarter = Q1_quarter - 1.5 * IQR_quarter,
                    upper_bound_quarter = Q3_quarter + 1.5 * IQR_quarter,
                    is_outlier_quarter = numeric_variable < lower_bound_quarter | numeric_variable > upper_bound_quarter,
                    numeric_variable = ifelse(is_outlier_quarter, median(numeric_variable[!is_outlier_quarter], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
            
            # Segunda capa imputación: atípicos generales
            temp_df <- temp_df %>%
                group_by(hour) %>%
                mutate(
                    Q1_global = quantile(numeric_variable, 0.25, na.rm = TRUE),
                    Q3_global = quantile(numeric_variable, 0.75, na.rm = TRUE),
                    IQR_global = Q3_global - Q1_global,
                    lower_bound_global = Q1_global - 1.5 * IQR_global,
                    upper_bound_global = Q3_global + 1.5 * IQR_global,
                    is_outlier_global = numeric_variable < lower_bound_global | numeric_variable > upper_bound_global,
                    numeric_variable = ifelse(is_outlier_global, median(numeric_variable[!is_outlier_global], na.rm = TRUE), numeric_variable)
                ) %>%
                ungroup()
            
            # Porcentaje total de outliers (o equivalentemente % total imputado)
            temp_df$is_outlier_total <- ifelse(temp_df$is_outlier_quarter | temp_df$is_outlier_global, TRUE, FALSE)
            pct_total_imputado <- sum(temp_df$is_outlier_total, na.rm = T)/nrow(temp_df)
            
            # Si el % total de outliers (o % total de valores imputados) es de > 15% la serie se elimina
            #ifelse()
        } 
        
        # Estandarización a media 0 y varianza 1
        if(standarize == TRUE){
            temp_df$numeric_variable_standardized <- (temp_df$numeric_variable - mean(temp_df$numeric_variable, na.rm = T))/sd(temp_df$numeric_variable, na.rm = T)
            lista_resultados <- list(temp_df$numeric_variable_standardized, pct_total_imputado)
            #return(temp_df$numeric_variable_standardized)
            return(lista_resultados)
        } else {
            lista_resultados <- list(temp_df$numeric_variable, pct_total_imputado)
            return(lista_resultados)
        }
        
    }
    
    # Aplicar la función interna a todas las columnas numéricas
    #vector_atipicos <- vector(mode = "numeric", length = length(numeric_cols))
    #names(vector_atipicos) <- numeric_cols
    for (col in numeric_cols) {
        resultados <- process_column(df[[col]], df$tstp, df$is_home)
        df[[col]] <- as.vector(resultados[[1]])
        if(col == "energy_kwh_hh"){
            df$pct_imputado_energy <- resultados[[2]]
        }
        #IQR_global = IQR(df[[col]], na.rm = TRUE)
        #lower_bound_global = quantile(df[[col]], 0.25, na.rm = T) - 1.5 * IQR_global
        #upper_bound_global = quantile(df[[col]], 0.75, na.rm = T) + 1.5 * IQR_global
        #is_outlier_prop = sum(df[[col]] < lower_bound_global | df[[col]] > upper_bound_global, na.rm = T)/nrow(df)
        #vector_atipicos[names(vector_atipicos) == col] <- is_outlier_prop
    }
    
    df_preprocessed <- df %>% select(everything(), -hour, -week, -month, -year, -is_home)
    return(df_preprocessed)
    # Devolver lista con df imputado y vector de porcentaje de atípicos por columna una vez estandarizado
    #lista_resultados <- list(df_preprocessed, vector_atipicos)
    #names(lista_resultados) <- c("df_preprocessed", "proporcion_atipicos")
    #return(lista_resultados)
    
}

preprocess_GCC(datos_hourly)


## Ejecuto la función para todos los archivos y los guardo en una carpeta de archivos estacionarios
## En paralelo dentro de cada archivo por ids (HECHO)
{
    input_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME"
    #output_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary"
    output_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary_v2"
    
    # Crear el directorio de salida si no existe
    if (!dir.exists(output_dir)) {
        dir.create(output_dir)
    }
    
    # Listar todos los archivos CSV en el directorio de entrada
    files <- list.files(input_dir, pattern = "hourly_clean_def_block_.*\\.csv", full.names = TRUE)
    #files <- files[11]
    
    # Función Notin
    `%notin%` <- Negate(`%in%`)
    
    # Función para procesar cada split
    process_split <- function(split_data) {
        preprocess_GCC(split_data, transformation = "mstl")
    }
    
    # Función para procesar un archivo y guardarlo
    process_stationarity_files <- function(file) {
        # Obtener el nombre base del archivo (sin la ruta)
        file_name <- basename(file)
        
        # Generar el nombre de salida
        output_file <- gsub("\\.csv$", "_stationary.csv", file_name)
        output_path <- file.path(output_dir, output_file)
        
        # Procesar el archivo
        datos_hourly <- read_csv(file, 
                                 col_types = cols(`energy_kwh_hh` = col_number(),
                                                  lc_lid = col_character()))
        
        # Eliminamos los ID's que tienen menos de 6 meses informados
        eliminar_ids <- datos_hourly %>%
            group_by(lc_lid) %>%
            summarise(count = n()) %>%
            filter(count < 24 * 30 * 6) %>%
            pull(lc_lid)
        
        datos_hourly <- datos_hourly %>%
            filter(lc_lid %notin% eliminar_ids) %>%
            select(lc_lid, tstp, energy_kwh_hh, 
                   temperature_2m_c, apparent_temperature_c, 
                   relative_humidity_2m_percent, pressure_msl_h_pa, is_home,
                   stdorToU, Acorn, Acorn_grouped, file, season) %>%
            split(.$lc_lid)
        
        # Detectar el número de núcleos disponibles
        num_cores <- detectCores() - 5
        #num_cores <- 1
        # Procesar los splits en paralelo
        datos_hourly <- mclapply(datos_hourly, process_split, mc.cores = num_cores) %>%
            bind_rows()
        
        # Guardar el archivo procesado
        write_csv(datos_hourly, output_path)
    }
    
    # Procesar los archivos en paralelo (uno por vez pero con paralelización interna)
    mclapply(files, process_stationarity_files, mc.cores = 1) # Usar 1 para la paralelización interna
}




## --------------------------------------------------------------------------------
## ------------------------ Función para calcular el GCC() ------------------------
## --------------------------------------------------------------------------------
GCC <- function(x, y, k, method = "pearson", use = "pairwise.complete.obs") {
    # Tamaño de las series
    n <- length(x)
    
    # Inicializar matrices
    R_xx <- matrix(0, nrow = k + 1, ncol = k + 1)
    R_yy <- matrix(0, nrow = k + 1, ncol = k + 1)
    C_xy <- matrix(0, nrow = k + 1, ncol = k + 1)
    
    # Rellenar matrices
    for (i in 0:k) {
        for (j in 0:k) {
            if (i == j) {
                R_xx[i + 1, j + 1] <- 1
                R_yy[i + 1, j + 1] <- 1
                C_xy[i + 1, j + 1] <- cor(x, y, method = method, use = use)
            } else {
                h = abs(i - j) #max(c(i, j)) - min(c(i, j))
                R_xx[i + 1, j + 1] <- cor(x[(1):(n - h)], x[(1 + h):(n)], method = method, use = use)
                R_yy[i + 1, j + 1] <- cor(y[(1):(n - h)], y[(1 + h):(n)], method = method, use = use)
                if (i > j) {
                    C_xy[i + 1, j + 1] <- cor(x[(1):(n - h)], y[(1 + h):(n)], method = method, use = use)
                } else {
                    C_xy[i + 1, j + 1] <- cor(x[(1 + h):n], y[1:(n - h)], method = method, use = use)
                }
            }
        }
    }
    
    # Manera 1:
    #det_R_yx <- det(R_xx) * det(R_yy - C_xy %*% solve(R_xx) %*% t(C_xy))
    #GCC <- 1 - (det_R_yx/(det(R_xx)*det(R_yy)))^(1/(k+1))
    
    
    # Manera 2
    R_yy_mod <- R_yy - C_xy %*% solve(R_xx) %*% t(C_xy)
    
    
    det_R_xx <- det(R_xx)
    det_R_yy <- det(R_yy) # da valores muy pequeños porq hay mucha autocorrelacion de temperaturas?¿
    det_R_yy_mod <- det(R_yy_mod)
    
    GCC <- 1 - (det_R_yy_mod / det_R_yy)^(1/(k + 1))
    
    return(GCC)
}


# Ejemplo de uso
set.seed(123)
x <- rnorm(1000, 3, 2)
y <- 0.75 + x + x^2 + 3*x^4
x <- scale(x)
y <- scale(y)
GCC(x, y, k = 1, method = "pearson") # 0.8692 
GCC(x, y, k = 1, method = "spearman") # 0.99 


## Me da valores muy pequeños nose porque (revisar pre procesamiento)
## GCC de serie transformada por MSTL y estandarizada
datos_hourly_filtered <- datos_hourly %>% filter(lc_lid == "MAC001715")
datos_hourly_filtered_preprocessed <- preprocess_GCC(datos_hourly_filtered)

GCC(x = datos_hourly_filtered_preprocessed$energy_kwh_hh, 
    y = datos_hourly_filtered_preprocessed$temperature_2m_c, 
    k = 168, method = "spearman", use = "pairwise.complete.obs")

### GCC de serie transformada por diferencia estacional y estandarizada
GCC(x = datos_hourly_hogar_preprocessed_seasonaldiff$energy_kwh_hh, 
    y = datos_hourly_hogar_preprocessed_seasonaldiff$temperature_2m_c, 
    k = 24, method = "spearman", use = "pairwise.complete.obs")

## --------------------------------------------------------------------------------------------------
## -------------------------------- CLUSTERING v2 (distancia de gower) ------------------------------
## --------------------------------------------------------------------------------------------------
{
    # AQUÍ ABAJO
    # GENERALIZAR PARA QUE ACEPTE TODOS LOS IDS, Y VER SI SE PUEDE PARALELIZAR
    input_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME"
    output_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary_v2"
    archivos <- list.files(output_dir, full.names = TRUE)
    #archivos <- archivos[1]
    #data_list <- vector(mode = "list", length = length(archivos))
    #for (i in 1:length(archivos)) {
    # Leer el archivo CSV y almacenarlo en la lista de datos
    #data_list[[i]] <- read_csv(archivos[i], 
    #col_types = cols(`energy_kwh_hh` = col_number(),
    #lc_lid = col_character()))
    #data_list[[i]] <- data_list[[i]] %>% filter(year(data_list[[i]]$tstp) == 2013)
    #}
    # Combinar todos los datos en un solo data frame
    #datos_hourly <- do.call(rbind, data_list)
    #rm(data_list)
    
    library(future.apply)
    
    # Configura el plan de paralelización
    plan(multisession, workers = availableCores() - 7)
    
    # Función para leer y procesar cada archivo
    read_archivos <- function(file) {
        data <- read_csv(file, 
                         col_types = cols(`energy_kwh_hh` = col_number(),
                                          lc_lid = col_character()))
        return(data)
    }
    
    # Usar future_lapply para paralelizar el proceso
    data_list <- future_lapply(archivos, read_archivos)
    datos_hourly <- bind_rows(data_list)
    rm(data_list)
    
    # Porcentaje de NA's imputados en cada serie
    datos_histograma_imputados <- datos_hourly %>% group_by(lc_lid) %>% summarise(pct_imputado_energy = mean(pct_imputado_energy, na.rm = T))
    
    pct_90 <- quantile(datos_histograma_imputados$pct_imputado_energy, 0.90, na.rm = TRUE)
    
    # Crear el histograma con la línea vertical en el tercer cuartil
    histograma_pct_atipicos <- ggplot(data = datos_histograma_imputados) + 
        geom_histogram(aes(x = pct_imputado_energy), color = "black", fill = "#FF6666") +
        geom_vline(aes(xintercept = pct_90), color = "black", linetype = "dashed", size = 0.5, alpha = 0.5) +
        labs(title = "Histograma de proporción de atípicos detectados",
             subtitle = "Series de energía estacionarias - MSTL aplicado",
             x = "Proporción de atípicos detectados",
             y = "Frecuencia Absoluta") + 
        theme_minimal() +
        theme(legend.position = "top",
              axis.title = element_text(size = 12),
              axis.text = element_text(size = 11),
              plot.title = element_text(size = 14),
              plot.subtitle = element_text(size = 13))
    
    
    
    ids_eliminar_exceso_nas_imputados <- datos_hourly %>% filter(pct_imputado_energy > pct_90) %>% pull(lc_lid) %>% unique()
    total_series_eliminadas_atipicos <- length(ids_eliminar_exceso_nas_imputados)
    `%notin%` <- Negate(`%in%`)
    datos_hourly <- datos_hourly %>% filter(lc_lid %notin% ids_eliminar_exceso_nas_imputados)
    
    #datos_hourly_sinfiltrar <- datos_hourly
    #datos_hourly_filtrado <- datos_hourly %>% filter(stdorToU == "Std")
    #datos_hourly <- datos_hourly_filtrado
    #
    #datos_hourly <- read_csv("./Datos/archive/hourly_dataset_clean_ISHOME_stationary/hourly_clean_def_block_0_stationary.csv", 
    #col_types = cols(`energy_kwh_hh` = col_number(),
    #lc_lid = col_character()))
    #datos_hourly <- datos_hourly %>% filter(year(datos_hourly$tstp) == 2013)
    
    autocovarianza_cuantilica <- function(X, tau, tau_prime, l) {
        T <- length(X)
        if (l >= T) {
            stop("El lag l debe ser menor que la longitud de la serie de tiempo.")
        }
        
        # Calcular los cuantiles
        q_tau <- quantile(X, tau, na.rm = T)
        q_tau_prime <- quantile(X, tau_prime, na.rm = T)
        
        # Inicializar la suma
        suma <- 0
        
        # Calcular la suma
        for (t in 1:(T - l)) {
            I_t <- ifelse((X[t] <= q_tau) & (!is.na(X[t])), 1, 0)
            I_t_l <- ifelse((X[t + l] <= q_tau_prime) & (!is.na(X[t + l])), 1, 0)
            suma <- suma + I_t * I_t_l
        }
        
        # Calcular la autocovarianza cuantílica
        gamma_l <- unname((1 / (T - l)) * suma - tau * tau_prime)
        
        return(gamma_l)
    }
    
    #autocovarianza_cuantilica(X = datos_hourly$energy_kwh_hh, tau = 0.1, tau_prime = 0.3, l = 1)
    
    
    # Función para calcular la matriz de autocovarianzas cuantílicas para todos los lags
    matriz_autocovarianzas_cuantilicas <- function(X, cuantiles, L) {
        r <- length(cuantiles)
        
        # Crear una matriz de autocovarianzas
        autocovarianzas_matriz <- matrix(NA, nrow = L, ncol = r^2)
        
        # Usar `outer` para calcular todas las combinaciones de cuantiles
        for (l in 1:L) {
            autocovarianzas_matriz[l, ] <- as.vector(outer(cuantiles, cuantiles, Vectorize(function(tau, tau_prime) {
                autocovarianza_cuantilica(X, tau, tau_prime, l)
            })))
        }
        
        gamma_representation <- t(autocovarianzas_matriz)
        return(gamma_representation)
    }
    
    
    # Clustering jerárquico paralelizado
    # Número de núcleos para paralelizar
    num_cores <- detectCores() - 1
    
    # Configurar el clúster de procesamiento paralelo
    cl <- makeCluster(num_cores)
    registerDoParallel(cl)
    
    # Procesamiento paralelo de cada lc_lid
    Gamma_u <- foreach(data = split(datos_hourly, datos_hourly$lc_lid), .combine = cbind, .packages = c("dplyr")) %dopar% {
        id <- unique(data$lc_lid)
        gamma_matrix <- matriz_autocovarianzas_cuantilicas(data$energy_kwh_hh, cuantiles = c(0.1, 0.5, 0.9), L = 1)
        colnames(gamma_matrix) <- id
        gamma_matrix
    }
    
    # Detener el clúster paralelo
    stopCluster(cl)
    rownames(Gamma_u) <- c("0.1|0.1", "0.1|0.5", "0.1|0.9", "0.5|0.1", "0.5|0.5", "0.5|0.9", "0.9|0.1", "0.9|0.5", "0.9|0.9")
    
    
    
    # Añadimos las variables: stdorTou, Acorn_grouped y calculamos la matriz de distancias de Gower
    Gamma_u_expandida <- cbind(colnames(Gamma_u), as.data.frame(t(Gamma_u)))
    colnames(Gamma_u_expandida)[1] <- "lc_lid"
    Gamma_u_expandida$file <- NULL
    rownames(Gamma_u_expandida) <- Gamma_u_expandida$lc_lid
    Gamma_u_expandida$lc_lid <- NULL
    
    
    ## Matriz de distancias euclidea
    matriz_distancias <- daisy(Gamma_u_expandida, metric = "euclidean")
    hc <- hclust(as.dist(matriz_distancias), method = "ward.D2")  # Puedes cambiar el método según tus necesidades
    k = 2
    h = 0.7
    dendrograma <- color_branches(as.dendrogram(hc), h = h) #k = 5
    
    plot(dendrograma, nodePar = NULL, ylab = "Distancia Euclídea entre Clusters", 
         leaflab = "none", main = "Dendrograma de hogares antes de \nla eliminación de clusters atípicos (Vínculo de Ward)")
    abline(h = h, lty = 2)
    
    # Cortar el dendrograma para obtener los clusters con una distancia menor a 0.025
    clusters <- cutree(hc, h = h)
    
    # Calcular las siluetas
    sil <- silhouette(clusters, as.dist(matriz_distancias))
    summary(sil)
    
    # Graficar el silhouette plot
    plot(sil)
    fviz_silhouette(sil)
    
    # Identificar los clusters atípicos (con pocos)
    outliers <- names(which(table(clusters) < 100))
    clusters[clusters %in% outliers] <- "outlier"
    
    # Renombrar los clusters restantes para que la numeración tenga sentido
    cluster_ids <- unique(clusters)
    cluster_ids <- cluster_ids[cluster_ids != "outlier"]
    new_cluster_names <- setNames(seq_along(cluster_ids), cluster_ids)
    clusters[clusters != "outlier"] <- new_cluster_names[clusters[clusters != "outlier"]]
    
    # Convertir los nombres de clusters en factor para ordenar correctamente
    clusters <- factor(clusters, levels = c("outlier", unique(new_cluster_names)))
    
    # Imprimir los clusters y las observaciones atípicas
    print(clusters)
    print(atypical_observations <- names(clusters[clusters == "outlier"]))
    
    # Crear una lista con los ids de cada cluster
    cluster_list <- split(names(clusters), clusters)
    outlier_cluster_ids <- cluster_list[["outlier"]]
    
    # Imprimir la lista de clusters
    #print(cluster_list)
    
    
    ## --------------------------------------------------------------
    ## Eliminar los outliers y volver a hacer clustering jerárquico
    ## --------------------------------------------------------------
    `%notin%` <- Negate(`%in%`)
    datos_hourly <- datos_hourly %>% filter(lc_lid %notin% cluster_list[["outlier"]])
    
    # Clustering jerárquico paralelizado
    # Número de núcleos para paralelizar
    num_cores <- detectCores() - 1
    
    # Configurar el clúster de procesamiento paralelo
    cl <- makeCluster(num_cores)
    registerDoParallel(cl)
    
    # Procesamiento paralelo de cada lc_lid
    Gamma_u <- foreach(data = split(datos_hourly, datos_hourly$lc_lid), .combine = cbind, .packages = c("dplyr")) %dopar% {
        id <- unique(data$lc_lid)
        gamma_matrix <- matriz_autocovarianzas_cuantilicas(data$energy_kwh_hh, cuantiles = c(0.1, 0.5, 0.9), L = 1)
        colnames(gamma_matrix) <- id
        gamma_matrix
    }
    
    # Detener el clúster paralelo
    stopCluster(cl)
    rownames(Gamma_u) <- c("0.1|0.1", "0.1|0.5", "0.1|0.9", "0.5|0.1", "0.5|0.5", "0.5|0.9", "0.9|0.1", "0.9|0.5", "0.9|0.9")
    
    # Añadimos las variables: stdorTou, Acorn_grouped y calculamos la matriz de distancias de Gower
    #informations_households <- read_csv("./Datos/archive/informations_households.csv")
    Gamma_u_expandida <- cbind(colnames(Gamma_u), as.data.frame(t(Gamma_u)))
    colnames(Gamma_u_expandida)[1] <- "lc_lid"
    #Gamma_u_expandida <- left_join(Gamma_u_expandida, informations_households, by = join_by(lc_lid == LCLid))
    Gamma_u_expandida$file <- NULL
    rownames(Gamma_u_expandida) <- Gamma_u_expandida$lc_lid
    Gamma_u_expandida$lc_lid <- NULL
    #Gamma_u_expandida$stdorToU <- as.factor(Gamma_u_expandida$stdorToU)
    #Gamma_u_expandida$Acorn_grouped <- as.factor(Gamma_u_expandida$Acorn_grouped)
    #Gamma_u_expandida$Acorn <- as.factor(Gamma_u_expandida$Acorn)
    
    
    ## Matriz de distancias euclídea
    matriz_distancias <- daisy(Gamma_u_expandida, metric = "euclidean")
    hc <- hclust(as.dist(matriz_distancias), method = "ward.D2")  # Puedes cambiar el método según tus necesidades
    h = 0.5 #h = 0.6 - 1.2 notbad
    k = 4
    dendrograma <- color_branches(as.dendrogram(hc), h = h) #k = 5
    
    plot(dendrograma, nodePar = NULL, ylab = "Distancia Euclídea entre Clusters", 
         leaflab = "none", main = "Dendrograma de hogares después de \nla eliminación del cluster atípico (Vínculo de Ward)")
    abline(h = h, lty = 2)
    
    # Cortar el dendrograma para obtener los clusters con una distancia menor a 0.025
    clusters <- cutree(hc, h = h)
    
    # Calcular las siluetas
    
    sil <- silhouette(clusters, as.dist(matriz_distancias))
    summary(sil)
    
    # Graficar el silhouette plot
    plot(sil)
    fviz_silhouette(sil)
    
    ## GUARDAR A QUÉ CLUSTER PERTENECE CADA ID Y LOS IDS ELIMINADOS (CLUSTER OUTLIERS)
    # Crear una lista con los ids de cada cluster
    cluster_list <- split(names(clusters), clusters)
    informations_households <- read_csv("./Datos/archive/informations_households.csv")
    removed_datos_hourly_datacleaning <- read_csv("./Datos/archive/removed_datos_hourly_datacleaning.csv")
    informations_households$outlier_datacleaning <- ifelse(informations_households$LCLid %in% (removed_datos_hourly_datacleaning %>% 
                                                                                                   filter(motive == "Outlier Excess") %>% 
                                                                                                   pull(id_removed)), TRUE, FALSE)
    informations_households$NA_datacleaning <- ifelse(informations_households$LCLid %in% (removed_datos_hourly_datacleaning %>% 
                                                                                              filter(motive == "NA excess") %>% 
                                                                                              pull(id_removed)), TRUE, FALSE) # rellenar este y el d arriba con la informacion q saque mañana al ejecutar el script en el servidor
    informations_households$outlier_preprocessing <- ifelse(informations_households$LCLid %in% ids_eliminar_exceso_nas_imputados, TRUE, FALSE)
    informations_households$outlier_clustering <- ifelse(informations_households$LCLid %in% outlier_cluster_ids, TRUE, FALSE)
    
    informations_households$remove <- ifelse((informations_households$outlier_datacleaning) | 
                                                 (informations_households$NA_datacleaning) | 
                                                 (informations_households$outlier_preprocessing) | 
                                                 (informations_households$outlier_clustering), TRUE, FALSE)
    
    informations_households$cluster <- ifelse(informations_households$LCLid %in% cluster_list[[1]], "1", 
                                              ifelse(informations_households$LCLid %in% cluster_list[[2]], "2", 
                                                     ifelse(informations_households$LCLid %in% cluster_list[[3]], "3", 
                                                            ifelse(informations_households$LCLid %in% cluster_list[[4]], "4", 
                                                                   ifelse(informations_households$LCLid %in% cluster_list[[5]], "5", 
                                                                          NA)))))
    
    informations_households$remove <- ifelse(informations_households$Acorn_grouped %in% c("ACORN-", "ACORN-U"), TRUE, informations_households$remove)
    
    
    write_csv(informations_households, "./Datos/archive/informations_households_after_clustering.csv") 
    informations_households$remove %>% table() # despues de eliminar todos los atipicos y na's nos quedamos con 4202 series, 
    1421/5566 #es decir en total se ha eliminado un 24.5% de los hogares
    
    # Comparar cluster vs acorn group (a través de informations_households no del dataset entero datos_hourly)
    # 
    informations_households <- read_csv("./Datos/archive/informations_households_after_clustering.csv")
    data_filtered <- informations_households %>% filter(remove == FALSE)
    # Existen diferencias entre clusters (rechazamos H_0: los clusters provienen de la misma población)
    
    ## Cluster vs Acorn Grouped
    tab <- tabyl(dat = data_filtered, cluster, Acorn_grouped, show_na = F)
    chisq.test(tab) 
    cont_table_Acorngrouped <- table(data_filtered[["cluster"]], 
                                     data_filtered[["Acorn_grouped"]], useNA = "no")
    cramers_v_Acorngrouped <- cramerV(cont_table_Acorngrouped, bias.correct = T)
    
    ## Cluster vs Acorn
    cont_table_Acorn <- table(data_filtered[["cluster"]], 
                              data_filtered[["Acorn"]], useNA = "no")
    cramers_v_Acorn <- cramerV(cont_table_Acorn, bias.correct = T)
    
    ## Cluster vs Tariff (independiente)
    tab <- tabyl(dat = data_filtered, cluster, stdorToU, show_na = F)
    chisq.test(tab) 
    cont_table_tariff <- table(data_filtered[["cluster"]], data_filtered[["stdorToU"]], useNA = "no")
    cramers_v_tariff <- cramerV(cont_table_tariff, bias.correct = T)
    
    ## Cluster vs block (independiente)
    cont_table_file <- table(data_filtered[["cluster"]], 
                             data_filtered[["file"]], useNA = "no")
    cramers_v_file <- cramerV(cont_table_file, bias.correct = T)
    
    ## Sacar tablas en formato Latex
    
    
    # Obtener datos_hourly
    informations_households <- read_csv("./Datos/archive/informations_households_after_clustering.csv")
    input_dir <- "./Datos/archive/hourly_dataset_clean_ISHOME"
    archivos <- list.files(input_dir, full.names = TRUE)
    data_list <- vector(mode = "list", length = length(archivos))
    for (i in 1:length(archivos)) {
        # Leer el archivo CSV y almacenarlo en la lista de datos
        data_list[[i]] <- read_csv(archivos[i], 
                                   col_types = cols(`energy_kwh_hh` = col_number(),
                                                    lc_lid = col_character()))
        #data_list[[i]] <- data_list[[i]] %>% filter(year(data_list[[i]]$tstp) == 2013)
    }
    
    # Combinar todos los datos en un solo data frame
    datos_hourly <- bind_rows(data_list)
    rm(data_list)
    
    
    ## Sacar gráficos boxplots de clusters (agrupar por hora en cada id y sacar boxplot x horas de cada cluster)
    datos_hourly <- left_join(datos_hourly, informations_households, by = join_by(lc_lid == LCLid))
    datos_hourly$hour <- hour(datos_hourly$tstp) %>% as.factor()
    datos_hourly$cluster <- as.factor(datos_hourly$cluster)
    datos_hourly <- datos_hourly %>% filter((remove == FALSE) & !is.na(cluster))
    
    get_mode <- function(v) {
        uniqv <- unique(v)
        uniqv[which.max(tabulate(match(v, uniqv)))]
    }
    
    datos_hourly_agg  <- datos_hourly %>% filter(is_home == TRUE) %>%
        group_by(lc_lid, hour) %>% 
        summarise(energy_kwh_hh = median(energy_kwh_hh, na.rm = TRUE),
                  cluster = get_mode(cluster),
                  Acorn_grouped = get_mode(Acorn_grouped.x),
                  tariff = get_mode(stdorToU.x))
    
    boxplots_consumo_clusters_horas <- ggplot(data = datos_hourly_agg) + 
        geom_boxplot(aes(x = hour, y = energy_kwh_hh),
                     outlier.colour = "red", outlier.shape = 16, outlier.size = 0.2, alpha = 0.8) + 
        facet_wrap(~ cluster) + theme_minimal() + 
        labs(title = "Boxplot de Consumo Energético por horas",
             subtitle = "en función de clusters",
             x = "Hora del día",
             y = "Consumo Energético (kWh)") + 
        theme_minimal() +
        theme(legend.position = "top",
              axis.title = element_text(size = 12),
              axis.text = element_text(size = 11),
              plot.title = element_text(size = 14),
              plot.subtitle = element_text(size = 13)) +
        scale_x_discrete(breaks = seq(0, 47, by = 3)) + ylim(c(0, 1.2))
    
    boxplots_consumo_acorn_horas <- ggplot(data = datos_hourly_agg) + 
        geom_boxplot(aes(x = hour, y = energy_kwh_hh),
                     outlier.colour = "red", outlier.shape = 16, outlier.size = 0.2, alpha = 0.8) + 
        facet_wrap(~ Acorn_grouped) + theme_minimal() + 
        labs(title = "Boxplot de Consumo Energético por horas",
             subtitle = "en función de grupos Acorn",
             x = "Hora del día",
             y = "Consumo Energético (kWh)") + 
        theme_minimal() +
        theme(legend.position = "top",
              axis.title = element_text(size = 12),
              axis.text = element_text(size = 11),
              plot.title = element_text(size = 14),
              plot.subtitle = element_text(size = 13)) +
        scale_x_discrete(breaks = seq(0, 47, by = 3)) + ylim(c(0, 1.5))
    
    boxplots_consumo_tariff_horas <- ggplot(data = datos_hourly_agg) + 
        geom_boxplot(aes(x = hour, y = energy_kwh_hh),
                     outlier.colour = "red", outlier.shape = 16, outlier.size = 0.2, alpha = 0.8) + 
        facet_wrap(~ tariff) + theme_minimal() + 
        labs(title = "Boxplot de Consumo Energético por horas",
             subtitle = "en función de tipo de Tarifa",
             x = "Hora del día",
             y = "Consumo Energético (kWh)") + 
        theme_minimal() +
        theme(legend.position = "top",
              axis.title = element_text(size = 12),
              axis.text = element_text(size = 11),
              plot.title = element_text(size = 14),
              plot.subtitle = element_text(size = 13)) +
        scale_x_discrete(breaks = seq(0, 47, by = 3)) + ylim(c(0, 1.5))
    
    ggplot(data = datos_hourly_agg) + geom_line(aes(x = hour, y = energy_kwh_hh, group = cluster))
    
    
}




## ---------------------------------------------------------------------------------
## --------------------------- RANKING A NIVEL SEMANAL -----------------------------
## ---------------------------------------------------------------------------------

# 1. Estacionalizamos todas las series a nivel horario (cada id representa una serie multivariante con energy y temperature por ejemplo)
# 2. Obtenemos una agregación a nivel hora/semana de cada serie:
## es decir estimamos como es una semana típica por cada ID, 
## es decir una matriz con 24*7 = 168 filas representando las horas/dia de todos los dias de una semana (lunes 01:00 hasta domingo 23:00)
## y tenga tantas columnas como semanas tenga ese ID informado
## Haciendo la media de cada fila obtendremos la media de las horas/dia por todas las semanas
## De esta forma eliminamos el ruido y mantenemos la estructura estacional a nivel semanal
# 3. Aplicamos GCC a cada serie agregada de cada ID
# 4. Ordenamos descendentemente y obtenemos el ranking buscado

# Función para obtener los archivos preprocesados

# Función para procesar los archivos y calcular los rankings
calcular_rankings <- function(k, variable_x, variable_y, archivos, by_seasons, nombre_x, nombre_y) {
    # Registrar cluster para procesamiento en paralelo
    num_cores <- detectCores() - 3
    cl <- makeCluster(num_cores)
    registerDoParallel(cl)
    
    # Función Notin
    `%notin%` <- Negate(`%in%`)
    
    # Cargamos informations_households
    ids_rankings <- read_csv("./Datos/archive/informations_households_after_clustering.csv") %>% 
        filter(remove == FALSE) %>% pull(LCLid)
    
    # Función para procesar cada archivo y calcular el GCC
    procesar_archivo <- function(archivo, k, variable_x, variable_y, by_seasons) {
        datos_hourly <- read_csv(archivo, 
                                 col_types = cols(`energy_kwh_hh` = col_number(),
                                                  lc_lid = col_character())) %>% 
            filter(lc_lid %in% ids_rankings)
        
        if(by_seasons == TRUE){
            # Obtenemos la semana típica de la temporada fría (24*7 = 168 filas)
            datos_hourly_agg_cold <- datos_hourly %>%
                filter((season %in% c("Winter", "Autumn", "Spring"))) %>%
                mutate(hour = hour(tstp),
                       day_of_week = wday(tstp, label = TRUE, week_start = 1)) %>% 
                group_by(lc_lid, day_of_week, hour) %>%
                summarise(!!sym(variable_x) := mean(.data[[variable_x]], na.rm = TRUE),
                          !!sym(variable_y) := mean(.data[[variable_y]], na.rm = TRUE)) %>%
                ungroup() %>% 
                arrange(lc_lid, match(day_of_week, c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")), hour) %>% 
                group_by(lc_lid) %>%
                mutate(!!sym(variable_x) := scale(.data[[variable_x]]),
                       !!sym(variable_y) := scale(.data[[variable_y]])) %>%
                ungroup()
            
            # Obtenemos la semana típica de la temporada cálida (24*7 = 168 filas)
            datos_hourly_agg_warm <- datos_hourly %>%
                filter(season == "Summer") %>%
                mutate(hour = hour(tstp),
                       day_of_week = wday(tstp, label = TRUE, week_start = 1)) %>% 
                group_by(lc_lid, day_of_week, hour) %>%
                summarise(!!sym(variable_x) := mean(.data[[variable_x]], na.rm = TRUE),
                          !!sym(variable_y) := mean(.data[[variable_y]], na.rm = TRUE)) %>%
                ungroup() %>% 
                arrange(lc_lid, match(day_of_week, c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")), hour) %>% 
                group_by(lc_lid) %>%
                mutate(!!sym(variable_x) := scale(.data[[variable_x]]),
                       !!sym(variable_y) := scale(.data[[variable_y]])) %>%
                ungroup()
            
            # Calcular el GCC para cada ID
            ranking_cold <- foreach(id = unique(datos_hourly_agg_cold$lc_lid), .combine = rbind, .packages = c("dplyr")) %dopar% {
                subset_data <- datos_hourly_agg_cold %>% filter(lc_lid == id)
                x <- subset_data[[variable_x]]
                y <- subset_data[[variable_y]]
                #gcc_value_pearson_cold <- GCC(x, y, k, method = "pearson")
                gcc_value_spearman_cold <- GCC(x, y, k, method = "spearman")
                #gcc_value_kendall_cold <- GCC(x, y, k, method = "kendall")
                
                data.frame(lc_lid = id, 
                           GCC_spearman_cold = gcc_value_spearman_cold)
            }
            
            ranking_warm <- foreach(id = unique(datos_hourly_agg_warm$lc_lid), .combine = rbind, .packages = c("dplyr")) %dopar% {
                subset_data <- datos_hourly_agg_warm %>% filter(lc_lid == id)
                x <- subset_data[[variable_x]]
                y <- subset_data[[variable_y]]
                #gcc_value_pearson_warm <- GCC(x, y, k, method = "pearson")
                gcc_value_spearman_warm <- GCC(x, y, k, method = "spearman")
                #gcc_value_kendall_warm <- GCC(x, y, k, method = "kendall")
                
                data.frame(lc_lid = id, 
                           GCC_spearman_warm = gcc_value_spearman_warm)
            }
            
            # Devolver lista
            return(list(cold = ranking_cold, warm = ranking_warm))
        } else {
            # Obtenemos la semana típica de la temporada fría (24*7 = 168 filas)
            datos_hourly_agg_cold <- datos_hourly %>%
                mutate(hour = hour(tstp),
                       day_of_week = wday(tstp, label = TRUE, week_start = 1)) %>% 
                group_by(lc_lid, day_of_week, hour) %>%
                summarise(!!sym(variable_x) := mean(.data[[variable_x]], na.rm = TRUE),
                          !!sym(variable_y) := mean(.data[[variable_y]], na.rm = TRUE)) %>%
                ungroup() %>% 
                arrange(lc_lid, match(day_of_week, c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")), hour) %>% 
                group_by(lc_lid) %>%
                mutate(!!sym(variable_x) := scale(.data[[variable_x]]),
                       !!sym(variable_y) := scale(.data[[variable_y]])) %>%
                ungroup()
            
            # Obtenemos la semana típica de la temporada cálida (24*7 = 168 filas)
            datos_hourly_agg_warm <- datos_hourly_agg_cold
            
            # Calcular el GCC para cada ID
            ranking_cold <- foreach(id = unique(datos_hourly_agg_cold$lc_lid), .combine = rbind, .packages = c("dplyr")) %dopar% {
                subset_data <- datos_hourly_agg_cold %>% filter(lc_lid == id)
                x <- subset_data[[variable_x]]
                y <- subset_data[[variable_y]]
                #gcc_value_pearson_cold <- GCC(x, y, k, method = "pearson")
                gcc_value_spearman_cold <- GCC(x, y, k, method = "spearman")
                #gcc_value_kendall_cold <- GCC(x, y, k, method = "kendall")
                
                data.frame(lc_lid = id, 
                           GCC_spearman_cold = gcc_value_spearman_cold)
            }
            
            ranking_warm <- ranking_cold
            
            # Devolver lista
            return(list(cold = ranking_cold, warm = ranking_warm))
        }
        
    }
    
    # Calcular rankings para todos los archivos en paralelo
    rankings <- foreach(archivo = archivos, .combine = function(a, b) {
        list(cold = bind_rows(a$cold, b$cold), warm = bind_rows(a$warm, b$warm))
    }, .packages = c("dplyr", "readr", "lubridate", "foreach", "purrr", "forecast", "doParallel"), .export = c("GCC")) %dopar% {
        procesar_archivo(archivo, k, variable_x, variable_y, by_seasons)
    }
    
    # Detener el cluster paralelo
    stopCluster(cl)
    
    # Leer informations_households
    informations_households <- read_csv("./Datos/archive/informations_households_after_clustering.csv")
    
    # Crear los rankings independientes
    if(by_seasons){
        ranking_cold <- rankings$cold %>%
            mutate(ranking_spearman = rank(-GCC_spearman_cold, ties.method = "min")) %>% 
            arrange(ranking_spearman)
        colnames(ranking_cold)[2] <- "GCC_spearman"
        ranking_cold$ranking_spearman <- factor(ranking_cold$ranking_spearman, levels = sort(unique(ranking_cold$ranking_spearman)))
        
        
        
        
        ranking_warm <- rankings$warm %>%
            mutate(ranking_spearman = rank(-GCC_spearman_warm, ties.method = "min")) %>% 
            arrange(ranking_spearman)
        colnames(ranking_warm)[2] <- "GCC_spearman"
        ranking_warm$ranking_spearman <- factor(ranking_warm$ranking_spearman, levels = sort(unique(ranking_warm$ranking_spearman)))
        
        
        
        # Gráficos comparativos por Acorn, clusters, etc
        informations_households <- read_csv("./Datos/archive/informations_households_after_clustering.csv")
        ranking_cold <- left_join(ranking_cold, informations_households, by = join_by(lc_lid == LCLid))
        ranking_warm <- left_join(ranking_warm, informations_households, by = join_by(lc_lid == LCLid))
        ranking_cold$cluster <- as.factor(ranking_cold$cluster)
        ranking_warm$cluster <- as.factor(ranking_warm$cluster)
        
        # Crear etiquetas para el eje x cada 100 posiciones
        breaks_cold <- seq(1, max(as.numeric(as.character(ranking_cold$ranking_spearman))), by = 500)
        breaks_warm <- seq(1, max(as.numeric(as.character(ranking_warm$ranking_spearman))), by = 500)
        
        # Gráfico de ranking
        # Crear el gráfico
        ranking_general_cold <- ggplot(ranking_cold, aes(x = ranking_spearman, y = GCC_spearman)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman (Temporada Fría)", breaks = breaks_cold) +
            scale_y_continuous("GCC Spearman") +
            labs(title = paste0("Ranking de hogares"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_general_warm <- ggplot(ranking_warm, aes(x = ranking_spearman, y = GCC_spearman)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman (Temporada Cálida)", breaks = breaks_warm) +
            scale_y_continuous("GCC Spearman") +
            labs(title = paste0("Ranking de hogares"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_acorn_cold <- ggplot(ranking_cold, aes(x = ranking_spearman, y = GCC_spearman, fill = Acorn_grouped)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks_cold) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Acorn Grouped",
                 title = paste0("Ranking de hogares (Temporada Fría)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_acorn_warm <- ggplot(ranking_warm, aes(x = ranking_spearman, y = GCC_spearman, fill = Acorn_grouped)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks_warm) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Acorn Grouped",
                 title = paste0("Ranking de hogares (Temporada Cálida)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_clustering_cold <- ggplot(ranking_cold, aes(x = ranking_spearman, y = GCC_spearman, fill = cluster)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks_cold) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Cluster",
                 title = paste0("Ranking de hogares (Temporada Fría)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_clustering_warm <- ggplot(ranking_warm, aes(x = ranking_spearman, y = GCC_spearman, fill = cluster)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks_warm) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Cluster",
                 title = paste0("Ranking de hogares (Temporada Cálida)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        
        # Visualización de la distribución de las correlaciones por grupo Acorn
        boxplot_acorn_grouped_cold <- ggplot(filter(ranking_cold, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn_grouped, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn (Temporada Fría)", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        boxplot_acorn_grouped_warm <- ggplot(filter(ranking_warm, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn_grouped, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn (Temporada Cálida)", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        # Visualización de la distribución de las correlaciones por Acorn
        boxplot_acorn_cold <- ggplot(filter(ranking_cold, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn (Temporada Fría)", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal() + coord_flip()
        
        boxplot_acorn_warm <- ggplot(filter(ranking_warm, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn (Temporada Cálida)", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal() + coord_flip()
        
        # Visualización de la distribución de las correlaciones por tipo de Tarifa
        boxplot_tariff_cold <- ggplot(ranking_cold, aes(x = stdorToU, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Tipo de Tarifa (Temporada Fría)", 
                 x = "Tipo de Tarifa",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        boxplot_tariff_warm <- ggplot(ranking_warm, aes(x = stdorToU, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Tipo de Tarifa (Temporada Cálida)", 
                 x = "Tipo de Tarifa",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        # Visualización de la distribución de las correlaciones por Cluster
        boxplot_cluster_cold <- ggplot(ranking_cold, aes(x = cluster, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Cluster (Temporada Fría)", 
                 x = "Cluster",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        boxplot_cluster_warm <- ggplot(ranking_warm, aes(x = cluster, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Cluster (Temporada Cálida)", 
                 x = "Cluster",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " y ", nombre_y)) +
            theme_minimal()
        
        # Densidad de los GCC's estimados
        density_GCC_cold <- ggplot(filter(ranking_cold, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), 
                                   aes(x = GCC_spearman, fill = Acorn_grouped)) +
            geom_density(alpha = 0.4) +
            labs(fill = "Acorn Group",
                 title = paste0("Densidades muestrales de GCC de Spearman"),
                 subtitle = paste0(nombre_x, " vs ", nombre_y, " (Temporada Fría)")) +
            theme_minimal() + 
            theme(legend.position = "top",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        density_GCC_warm <- ggplot(filter(ranking_warm, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), 
                                   aes(x = GCC_spearman, fill = Acorn_grouped)) +
            geom_density(alpha = 0.4) +
            labs(fill = "Acorn Group",
                 title = paste0("Densidades muestrales de GCC de Spearman"),
                 subtitle = paste0(nombre_x, " vs ", nombre_y, " (Temporada Cálida)")) +
            theme_minimal() + 
            theme(legend.position = "top",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        lista_resultados_cold <- list(ranking_acorn_cold, ranking_clustering_cold, boxplot_acorn_cold, boxplot_acorn_grouped_cold, boxplot_tariff_cold, boxplot_cluster_cold, density_GCC_cold)
        lista_resultados_warm <- list(ranking_acorn_warm, ranking_clustering_warm, boxplot_acorn_warm, boxplot_acorn_grouped_warm, boxplot_tariff_warm, boxplot_cluster_warm, density_GCC_warm)
        names(lista_resultados_cold) <- c("Ranking (por grupo Acorn)", "Ranking (por clusters)", 
                                          "Boxplot Acorn", "Boxplot Acorn Group", "Boxplot Tariff", "Boxplot Cluster", "Density GCC")
        names(lista_resultados_warm) <- c("Ranking (por grupo Acorn)", "Ranking (por clusters)", 
                                          "Boxplot Acorn", "Boxplot Acorn Group", "Boxplot Tariff", "Boxplot Cluster", "Density GCC")
        
        
        lista_resultados <- list(lista_resultados_cold, lista_resultados_warm)
        names(lista_resultados) <- c("Cold", "Warm")
        
        return(lista_resultados)
    } else {
        ranking <- rankings$cold %>%
            mutate(ranking_spearman = rank(-GCC_spearman_cold, ties.method = "min")) %>% 
            arrange(ranking_spearman)
        colnames(ranking)[2] <- "GCC_spearman"
        
        ranking$ranking_spearman <- factor(ranking$ranking_spearman, levels = sort(unique(ranking$ranking_spearman)))
        
        # Gráficos comparativos por Acorn, clusters, etc
        ranking <- left_join(ranking, informations_households, by = join_by(lc_lid == LCLid))
        ranking$cluster <- as.factor(ranking$cluster)
        
        
        # Crear etiquetas para el eje x cada 100 posiciones
        breaks <- seq(1, max(as.numeric(as.character(ranking$ranking_spearman))), by = 500)
        
        # Gráfico de ranking
        # Crear el gráfico
        ranking_general <- ggplot(ranking, aes(x = ranking_spearman, y = GCC_spearman)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks) +
            scale_y_continuous("GCC Spearman") +
            labs(title = paste0("Ranking de hogares"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_acorn <- ggplot(ranking, aes(x = ranking_spearman, y = GCC_spearman, fill = Acorn_grouped)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Acorn Grouped",
                 title = paste0("Ranking de hogares (en función de grupos Acorn)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_clustering <- ggplot(ranking, aes(x = ranking_spearman, y = GCC_spearman, fill = cluster)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Cluster",
                 title = paste0("Ranking de hogares (en función de clusters)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        ranking_tariff <- ggplot(ranking, aes(x = ranking_spearman, y = GCC_spearman, fill = stdorToU)) +
            geom_bar(stat = "identity") +
            scale_x_discrete("Ranking GCC de Spearman", breaks = breaks) +
            scale_y_continuous("GCC Spearman") +
            labs(fill = "Tipo de Tarifa",
                 title = paste0("Ranking de hogares (en función de Tipo de Tarifa)"),
                 subtitle = paste0("basado en GCC's entre ", nombre_x, " y ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "right",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        # Visualización de la distribución de las correlaciones por grupo Acorn
        boxplot_acorn_grouped <- ggplot(filter(ranking, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn_grouped, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        # Visualización de la distribución de las correlaciones por Acorn
        boxplot_acorn <- ggplot(filter(ranking, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), aes(x = Acorn, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Grupo Acorn", 
                 x = "Grupo Acorn",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal() + coord_flip()
        
        # Visualización de la distribución de las correlaciones por tipo de Tarifa
        boxplot_tariff <- ggplot(ranking, aes(x = stdorToU, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Tipo de Tarifa",
                 x = "Tipo de Tarifa",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        # Visualización de la distribución de las correlaciones por Cluster
        boxplot_cluster <- ggplot(ranking, aes(x = cluster, y = GCC_spearman)) +
            geom_boxplot() +
            labs(title = "GCC de Spearman por Cluster",
                 x = "Cluster",
                 y = "GCC de Spearman", 
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal()
        
        # Densidad de los GCC's estimados
        density_GCC <- ggplot(filter(ranking, Acorn_grouped %in% c("Adversity", "Affluent", "Comfortable")), 
                              aes(x = GCC_spearman, fill = Acorn_grouped)) +
            geom_density(alpha = 0.4) +
            labs(fill = "Acorn Group",
                 title = paste0("Densidades muestrales de GCC de Spearman"),
                 subtitle = paste0(nombre_x, " vs ", nombre_y)) +
            theme_minimal() + 
            theme(legend.position = "top",
                  axis.title = element_text(size = 12),
                  axis.text = element_text(size = 11),
                  plot.title = element_text(size = 14),
                  plot.subtitle = element_text(size = 13))
        
        lista_resultados <- list(ranking_acorn, ranking_clustering, boxplot_acorn, boxplot_acorn_grouped, boxplot_tariff, boxplot_cluster, density_GCC)
        names(lista_resultados) <- c("Ranking (por grupo Acorn)", "Ranking (por clusters)", 
                                     "Boxplot Acorn", "Boxplot Acorn Group", "Boxplot Tariff", "Boxplot Cluster", "Density GCC")
        
        return(lista_resultados)
    }
    
    
    
}

# Llamar a la función con los parámetros deseados
carpeta_entrada <- "./Datos/archive/hourly_dataset_clean_ISHOME_stationary_v2"
# Obtener la lista de archivos en la carpeta de entrada
archivos <- list.files(carpeta_entrada, full. = TRUE)
#archivos <- archivos[1] MAC001715
#archivos <- archivos[1:2]  # Solo para propósitos de demostración, puedes eliminar esta línea para procesar todos los archivos
#archivos <- "./Datos/archive/hourly_dataset_clean_ISHOME/hourly_clean_def_block_6.csv"

# FALTA SACAR LOS BOXPLOTS POR CLUSTERS Q TENIAN EL TITULO MAL PUESTO

resultados_temperature <- calcular_rankings(k = 24, variable_x = "energy_kwh_hh", 
                                            variable_y = "temperature_2m_c", 
                                            archivos = archivos, by_seasons = T,
                                            nombre_x = "Consumo energético",
                                            nombre_y = "Temperatura")

resultados_apparent_temperature <- calcular_rankings(k = 24, variable_x = "energy_kwh_hh", 
                                                     variable_y = "apparent_temperature_c", 
                                                     archivos = archivos, by_seasons = T,
                                                     nombre_x = "Consumo energético",
                                                     nombre_y = "Sensación Térmica")

resultados_humidity <- calcular_rankings(k = 24, variable_x = "energy_kwh_hh", 
                                         variable_y = "relative_humidity_2m_percent", 
                                         archivos = archivos, by_seasons = F,
                                         nombre_x = "Consumo energético",
                                         nombre_y = "Humedad Relativa")









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
library(vtable)
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
library(boot)
library(zoo)

# Lectura de datos
datos_hourly_agg <- read_csv("/home/rstudio-user/Datos/archive/datos_hourly_agg.csv")
datos_hourly_agg_acorn <- read_csv("/home/rstudio-user/Datos/archive/datos_hourly_agg_acorn.csv")

# Importamos funciones de preproceso y de GCC
datos_hourly_agg <- datos_hourly_agg %>% select(datetime, energy_kwh_hh, 
                                                temperature_2m_c, apparent_temperature_c, 
                                                relative_humidity_2m_percent, pressure_msl_h_pa) %>% rename(tstp = datetime)

preprocess_GCC_general <- function(df, transformation = "mstl", impute_outliers = TRUE, standarize = FALSE) {
        
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
        process_column <- function(series, datetime_series) {
                # Crear un data frame temporal
                temp_df <- data.frame(datetime = datetime_series, numeric_variable = series) %>% 
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
                #temp_df$numeric_variable <- ifelse(temp_df$is_home == FALSE, NA, temp_df$numeric_variable)
                
                
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
                        if(impute_outliers == TRUE){
                                lista_resultados <- list(temp_df$numeric_variable, pct_total_imputado)
                        } else{
                                lista_resultados <- list(temp_df$numeric_variable)
                        }
                        return(lista_resultados)
                }
                
        }
        
        # Aplicar la función interna a todas las columnas numéricas
        #vector_atipicos <- vector(mode = "numeric", length = length(numeric_cols))
        #names(vector_atipicos) <- numeric_cols
        for (col in numeric_cols) {
                resultados <- process_column(df[[col]], df$tstp)
                df[[col]] <- as.vector(resultados[[1]])
                if((impute_outliers == TRUE) & (col == "energy_kwh_hh")){
                        df$pct_imputado_energy <- resultados[[2]]
                }
                #IQR_global = IQR(df[[col]], na.rm = TRUE)
                #lower_bound_global = quantile(df[[col]], 0.25, na.rm = T) - 1.5 * IQR_global
                #upper_bound_global = quantile(df[[col]], 0.75, na.rm = T) + 1.5 * IQR_global
                #is_outlier_prop = sum(df[[col]] < lower_bound_global | df[[col]] > upper_bound_global, na.rm = T)/nrow(df)
                #vector_atipicos[names(vector_atipicos) == col] <- is_outlier_prop
        }
        
        df_preprocessed <- df %>% select(everything(), -hour, -week, -month, -year)
        return(df_preprocessed)
        # Devolver lista con df imputado y vector de porcentaje de atípicos por columna una vez estandarizado
        #lista_resultados <- list(df_preprocessed, vector_atipicos)
        #names(lista_resultados) <- c("df_preprocessed", "proporcion_atipicos")
        #return(lista_resultados)
        
}


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

# Obtenemos las series preprocesadas (a nivel horario y luego a nivel diario)
#datos_hourly_agg_preprocessed <- preprocess_GCC_general(datos_hourly_agg, transformation = "mstl", impute_outliers = FALSE, standarize = TRUE)
datos_daily_agg_preprocessed <- preprocess_GCC_general(datos_hourly_agg, transformation = "mstl", impute_outliers = TRUE, standarize = FALSE) %>%
        group_by(datetime = date(tstp)) %>% 
        summarize(across(where(is.numeric), mean)) %>%
        mutate(across(where(is.numeric), ~ scale(.) %>% as.vector)) #%>% select(-pct_imputado_energy)

# Sacar seasons de datos_daily_agg_preprocessed
#!!!
datos_daily_agg_preprocessed$season <- ifelse(month(datos_daily_agg_preprocessed$datetime) %in% c(3, 4, 5), "Spring", 
                                              ifelse(month(datos_daily_agg_preprocessed$datetime) %in% c(6, 7, 8), "Summer", 
                                                     ifelse(month(datos_daily_agg_preprocessed$datetime) %in% c(9, 10, 11), "Autumn", 
                                                            "Winter")))
datos_daily_agg_preprocessed$temporada <- ifelse(datos_daily_agg_preprocessed$season %in% c("Spring", "Autumn", "Winter"), "Temporada Fría", "Temporada Cálida")


# Ahora que ya tenemos las serie estacionarias y estandarizadas a 0-1, aplicamos el GCC con bootstrap a distintas variables y calculamos los resultados.


# Datos de ejemplo
{
        nombre_x = "energy_kwh_hh"
        nombre_y = "temperature_2m_c"
        k <- 7 # Número de retrasos 
        B_daily <- 1 # Número de replicaciones bootstrap
        block_length_daily <- round(nrow(datos_daily_agg_preprocessed)/4) # Longitud del bloque para el bootstrap por bloques
        
        # Realizar el bootstrap por bloques
        set.seed(123)  # Para reproducibilidad
        
        # Convertir el dataframe a un objeto zoo
        #datos_hourly_zoo <- zoo(datos_hourly_agg[, c("energy_kwh_hh", "temperature_2m_c")], order.by = datos_hourly_agg$tstp)
        datos_daily_zoo <- zoo(datos_daily_agg_preprocessed[, c(nombre_x, nombre_y)], order.by = datos_daily_agg_preprocessed$datetime)
        
        #boot_results_hourly <- tsboot(datos_hourly_zoo, statistic = gcc_stat, R = B_hourly, l = block_length_hourly, sim = "fixed", k = k)
        boot_results_daily <- tsboot(datos_daily_zoo, statistic = gcc_stat, nombre_x = "energy_kwh_hh", nombre_y = "temperature_2m_c", R = B_daily, l = block_length_daily, sim = "fixed", parallel = "snow", k = k)
        
        # Extraer los resultados
        #boot_samples_hourly <- boot_results_daily$t
        boot_samples_daily <- boot_results_daily$t
        
        # Visualizar los resultados
        hist(boot_samples, breaks = 30, main = "Distribución Bootstrap de GCC", xlab = "GCC", col = "skyblue")
}

# FUNCION BOOTSTRAP
dist_bootstrap_daily <- function(datos, x = "energy_kwh_hh", y = "temperature_2m_c", 
                                 nombre_x = "Consumo energético", nombre_y = "Temperatura", 
                                 B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = TRUE){
        # Número de nucleos para el procesamiento paralelo
        num_cores <- detectCores() - 5
        
        # Función que usa el bootstrap
        gcc_stat <- function(data, k, indices, x, y) {
                x <- data[indices, x]
                y <- data[indices, y]
                return(GCC(x, y, k, method = "spearman"))
        }
        
        
        if(by_seasons == FALSE){
                # Semilla aleatoria
                set.seed(123) 
                datos_zoo <- zoo(datos[, c(x, y)], order.by = datos$datetime)
                block_length_daily <- round(nrow(datos)/4, 0)
                boot_results_daily <- tsboot(datos_zoo, statistic = gcc_stat, x = x, y = y, R = B, 
                                             l = block_length_daily, sim = sim, parallel = "multicore", k = k, ncpus = num_cores)
                resultado_bootstrap <- boot_results_daily$t %>% as.data.frame() %>% rename(GCC = V1)
                intervalo_confianza <- quantile(resultado_bootstrap$GCC, probs = c(alpha/2, 1-alpha/2))
                
                # Gráfico 
                histograma_bootstrap <- ggplot(data = resultado_bootstrap, aes(x = GCC)) + 
                        geom_histogram(aes(y = after_stat(density)), fill = "white", color = "black", alpha = 0.5) +
                        geom_density(fill = "#FF6666", color = "black", alpha = 0.5) + 
                        theme_minimal() + 
                        labs(title = "Distribución bootstrap del GCC de Spearman",
                             subtitle = paste0(nombre_x, " vs ", nombre_y),
                             x = "GCC de Spearman", 
                             y = "Densidad") + 
                        theme(legend.position = "none",
                              plot.title = element_text(size = 14, face = "bold"),
                              plot.subtitle = element_text(size = 13),
                              axis.title = element_text(size = 12),
                              axis.text = element_text(size = 11)) 
                lista_resultados <- list(intervalo_confianza, histograma_bootstrap)
                names(lista_resultados) <- c("Intervalo confianza GCC", "Histograma Bootstrap")
                return(lista_resultados)
                
        } else {
                ## TEMPORADA FRÍA
                # Semilla aleatoria
                set.seed(123) 
                datos_cold <- datos %>% filter(temporada == "Temporada Fría")
                datos_zoo <- zoo(datos_cold[, c(x, y)], order.by = datos_cold$datetime)
                block_length_daily <- round(nrow(datos_zoo)/4, 0)
                boot_results_daily <- tsboot(datos_zoo, statistic = gcc_stat, x = x, y = y, R = B, 
                                             l = block_length_daily, sim = sim, parallel = "multicore", k = k, ncpus = num_cores)
                resultado_bootstrap <- boot_results_daily$t %>% as.data.frame() %>% rename(GCC = V1)
                intervalo_confianza <- quantile(resultado_bootstrap$GCC, probs = c(alpha/2, 1-alpha/2))
                
                # Gráfico 
                histograma_bootstrap <- ggplot(data = resultado_bootstrap, aes(x = GCC)) + 
                        geom_histogram(aes(y = after_stat(density)), fill = "white", color = "black", alpha = 0.5) +
                        geom_density(fill = "#FF6666", color = "black", alpha = 0.5) + 
                        theme_minimal() + 
                        labs(title = "Distribución bootstrap del GCC de Spearman",
                             subtitle = paste0(nombre_x, " vs ", nombre_y,  " | Temporada Fría"),
                             x = "GCC", 
                             y = "Densidad") + 
                        theme(legend.position = "none",
                              plot.title = element_text(size = 14, face = "bold"),
                              plot.subtitle = element_text(size = 13),
                              axis.title = element_text(size = 12),
                              axis.text = element_text(size = 11)) 
                lista_resultados_cold <- list(intervalo_confianza, histograma_bootstrap)
                names(lista_resultados_cold) <- c("Intervalo confianza GCC (Temporada Fría)", "Histograma Bootstrap (Temporada Fría)")
                
                ## TEMPORADA CÁLIDA
                # Semilla aleatoria
                set.seed(123) 
                datos_warm <- datos %>% filter(temporada == "Temporada Cálida")
                datos_zoo <- zoo(datos_warm[, c(x, y)], order.by = datos_warm$datetime)
                block_length_daily <- round(nrow(datos_zoo)/4, 0)
                boot_results_daily <- tsboot(datos_zoo, statistic = gcc_stat, x = x, y = y, R = B, 
                                             l = block_length_daily, sim = sim, parallel = "multicore", k = k, ncpus = num_cores)
                resultado_bootstrap <- boot_results_daily$t %>% as.data.frame() %>% rename(GCC = V1)
                intervalo_confianza <- quantile(resultado_bootstrap$GCC, probs = c(alpha/2, 1-alpha/2))
                
                # Gráfico 
                histograma_bootstrap <- ggplot(data = resultado_bootstrap, aes(x = GCC)) + 
                        geom_histogram(aes(y = after_stat(density)), fill = "white", color = "black", alpha = 0.5) +
                        geom_density(fill = "#FF6666", color = "black", alpha = 0.5) + 
                        theme_minimal() + 
                        labs(title = "Distribución bootstrap del GCC de Spearman",
                             subtitle = paste0(nombre_x, " vs ", nombre_y,  " | Temporada Cálida"),
                             x = "GCC", 
                             y = "Densidad") + 
                        theme(legend.position = "none",
                              plot.title = element_text(size = 14, face = "bold"),
                              plot.subtitle = element_text(size = 13),
                              axis.title = element_text(size = 12),
                              axis.text = element_text(size = 11)) 
                lista_resultados_warm <- list(intervalo_confianza, histograma_bootstrap)
                names(lista_resultados_warm) <- c("Intervalo confianza GCC (Temporada Cálida)", "Histograma Bootstrap (Temporada Cálida)")
                
                # Devolver ambas listas
                lista_final <- list(lista_resultados_cold, lista_resultados_warm)
                names(lista_final) <- c("Cold", "Warm")
                return(lista_final)
                
        }
        
}

bootstrap_temperature <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "energy_kwh_hh", y = "temperature_2m_c", 
                                              nombre_x = "Consumo energético", nombre_y = "Temperatura", 
                                              B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = TRUE)
bootstrap_sensaciontermica <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "energy_kwh_hh", y = "apparent_temperature_c", 
                                                   nombre_x = "Consumo energético", nombre_y = "Sensación térmica", 
                                                   B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = TRUE)
bootstrap_humidity <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "energy_kwh_hh", y = "relative_humidity_2m_percent", 
                                           nombre_x = "Consumo energético", nombre_y = "Humedad relativa", 
                                           B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = FALSE)

## Relaciones entre variables meteorológicas
bootstrap_temperature_sensaciontermica <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "temperature_2m_c", 
                                                               y = "apparent_temperature_c", 
                                                               nombre_x = "Temperatura", nombre_y = "Sensación Térmica", 
                                                               B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = FALSE)
bootstrap_temperature_humedad <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "temperature_2m_c", 
                                                      y = "relative_humidity_2m_percent", 
                                                      nombre_x = "Temperatura", nombre_y = "Humedad", 
                                                      B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = FALSE)
bootstrap_humedad_sensaciontermica <- dist_bootstrap_daily(datos = datos_daily_agg_preprocessed, x = "apparent_temperature_c", 
                                                           y = "relative_humidity_2m_percent", 
                                                           nombre_x = "Sensación Térmica", nombre_y = "Humedad", 
                                                           B = 10000, k = 7, alpha = 0.05, sim = "fixed", by_seasons = FALSE)
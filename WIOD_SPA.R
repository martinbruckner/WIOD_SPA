##########################################################################
##  WIOD Structural Path Analysis
##########################################################################

datapath <- "W:/WU/Projekte/SRU-Projekte/04_Daten/MRIO/IO data/WIOD/WIOD_Nov16/"
datapath <- "./input/"

library(openxlsx)
library(reshape2)
library(data.table)
library(parallel)

agg <- function(x)
{
  x <- as.matrix(x) %*% sapply(unique(colnames(x)),"==",colnames(x))
  return(x)
}
get_L2 <- function(fname, aL0, aL1, A, L1, cutoff){
  if(A[aL1,aL0] < cutoff) return(0)
  temp <- (extension * A)[,aL1] * A[aL1,aL0] * Y[aL0,country]
  temp <- data.frame(country = unique(class.col$Country)[country], L0 = aL0, L1 = aL1, L2 = 1:length(temp), L3 = 0, value = temp)
  try(fwrite(temp[abs(temp$value) > cutoff, ], file = fname, row.names = FALSE, col.names = FALSE, append = TRUE))
}

class.col <- read.xlsx(xlsxFile = paste0("./input/WIOD classification.xlsx"), sheet = 2)
class.row <- read.xlsx(xlsxFile = paste0("./input/WIOD classification.xlsx"), sheet = 1)
EXR <- read.xlsx(xlsxFile = paste0("./input/EXR_WIOD_Nov16.xlsx"), sheet = 2, startRow = 4)
SEA <- read.xlsx(xlsxFile = paste0("./input/WIOD_SEA_Nov16.xlsx"), sheet = 2)
SEA$country[SEA$country=="ROU"] <- "ROM"  # rename Romania
EMP <- SEA[SEA$variable=="EMP", ]
EMP$code <- class.col$IndustryGroup[1:nrow(EMP)]
EMP$description <- EMP$variable <- NULL
VA <- SEA[SEA$variable=="VA", ]
VA$code <- class.col$IndustryGroup[1:nrow(VA)]
VA$description <- VA$variable <- NULL
# convert VA into current US$
for(year in 2000:2014){
  VA[, as.character(year)] <- VA[, as.character(year)] * EXR[ match(VA$country,EXR$Acronym), as.character(year)]
}
# aggregate extensions
VA <- aggregate(. ~ code + country, VA, sum)
EMP <- aggregate(. ~ code + country, EMP, sum)


years <- 2010:2010
ncores <- 1

year <- years[1]
mclapply(years, mc.cores = ncores, function(year){
  load(paste0(datapath,"WIOT",year,"_October16_ROW.RData"))
  Z <- wiot[1:2464,6:(2464+5)]
  Y <- wiot[1:2464,(2465+5):2689]
  # x <- rowSums(wiot[1:2464,6:2689])
  
  # aggregate variables
  colnames(Z) <- paste(class.col$Country[1:ncol(Z)], class.col$IndustryGroup[1:ncol(Z)], sep = "_")
  Z <- agg(Z)
  Z <- t(Z)
  Y <- t(Y)
  colnames(Z) <- paste(class.col$Country[1:ncol(Z)], class.col$IndustryGroup[1:ncol(Z)], sep = "_")
  Z <- agg(Z)
  Z <- t(Z)
  colnames(Y) <- paste(class.col$Country[1:ncol(Y)], class.col$IndustryGroup[1:ncol(Y)], sep = "_")
  Y <- agg(Y)
  Y <- t(Y)
  
  colnames(Y) <- substr(colnames(Y), 1, 3)
  Y <- agg(Y)
  
  x <- rowSums(Z) + rowSums(Y)
  A <- t(t(Z)/x)
  A[! is.finite(A)] <- 0

  # Choose extension
  extension <- 1
  ext_name <- "pure"
  # extension <- c(as.numeric(VA[, as.character(year)]), rep(0,7)) / x
  # ext_name <- "VA"
  # extension <- c(as.numeric(EMP[, as.character(year)]), rep(0,7)) / x
  # ext_name <- "EMP"
  # extension[!is.finite(extension)] <- 0
  
  fname <- paste0("./output/results_",ext_name,"_", year, ".csv")
  fwrite(list("country","L0","L1","L2","L3","value"), file = fname, row.names = FALSE, col.names = FALSE)
  
  country <- 1
  for(country in 1:ncol(Y)){
    print(paste(year, "country", country, "/", 44))
    # Level 0
    results <- data.frame(country = unique(class.col$Country)[country], L0 = 1:nrow(Y), L1 = 0, L2 = 0, L3 = 0, value = (extension * Y[,country]))
    
    # Level 1
    L1 <- t(t(extension * A) * Y[,country])
    dimnames(L1) <- list(1:nrow(L1), 1:ncol(L1))
    
    temp <- melt(L1)
    colnames(temp) <- c("L1", "L0", "value")
    temp$L3 <- temp$L2 <- 0
    temp$country <- unique(class.col$Country)[country]
    temp <- temp[,c(6,2,1,4,5,3)]
    results <- rbind(results, temp)
    
    fwrite(results, file = fname, row.names = FALSE, col.names = FALSE, append = TRUE)
    
    # Level 2
    aL0 <- aL1 <- 1
    for(aL0 in 1:ncol(A)){
      # print(paste("sector",aL0,"/",ncol(A)))
      lapply(1:nrow(A), function(aL1) get_L2(fname, aL0, aL1, A, L1, cutoff = 0.1))
    }
  }
  
  zip::zip(paste0("./output/results_",ext_name,"_", year, ".zip"), files = fname)
  
  return(paste(year,"done"))
})


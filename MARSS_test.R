library(MARSS)
rm(list = ls())

df = read.csv('/Volumes/Samsung_T5/BitShares/ParsedData/aggregates/daily.USD_CNY.USD_BTS.CNY_BTS.csv.testit.csv', row.names = 1, header= TRUE)

## create vector of phytoplankton group names
column_names <- colnames(df)

## transpose data so time goes across columns
df_t <- t(df)
## get number of time series
N_ts <- dim(df_t)[1]
## get length of time series
TT <- dim(df_t)[2]

y_bar <- apply(df_t, 1, mean, na.rm = TRUE)
dat <- df_t - y_bar
rownames(dat) <- rownames(df_t)

spp <- rownames(df_t)
clr <- c("brown","blue","darkgreen","darkred","purple")
cnt <- 1
par(mfrow=c(N_ts,1), mai=c(0.5,0.7,0.1,0.1), omi=c(0,0,0,0))
for(i in spp){
  plot(dat[i,],xlab="",ylab="Abundance index", bty="L", xaxt="n", pch=16, col=clr[cnt], type="b")
  axis(1,12*(0:dim(dat_1980)[2])+1,yr_frst+0:dim(dat_1980)[2])
  title(i)
  cnt <- cnt + 1
}

## 'ZZ' is loadings matrix
Z_vals <- list("z11", 0, 0, "z21", "z22", 0, "z31", "z32", "z33", 
               "z41", "z42", "z43", "z51", "z52", "z53")
ZZ <- matrix(Z_vals, nrow = N_ts, ncol = 3, byrow = TRUE)
ZZ

## 'aa' is the offset/scaling
aa <- "zero"
## 'DD' and 'd' are for covariates
DD <- "zero"  # matrix(0,mm,1)
dd <- "zero"  # matrix(0,1,wk_last)
## 'RR' is var-cov matrix for obs errors
RR <- "diagonal and unequal"

## number of processes
mm <- 3
## 'BB' is identity: 1's along the diagonal & 0's elsewhere
BB <- "identity"  # diag(mm)
## 'uu' is a column vector of 0's
uu <- "zero"  # matrix(0,mm,1)
## 'CC' and 'cc' are for covariates
CC <- "zero"  # matrix(0,mm,1)
cc <- "zero"  # matrix(0,1,wk_last)
## 'QQ' is identity
QQ <- "identity"  # diag(mm)

## list with specifications for model vectors/matrices
mod_list <- list(Z = ZZ, A = aa, D = DD, d = dd, R = RR, B = BB, 
                 U = uu, C = CC, c = cc, Q = QQ)
## list with model inits
init_list <- list(x0 = matrix(rep(0, mm), mm, 1))
## list with model control parameters
con_list <- list(maxit = 3000, allow.degen = TRUE)
## fit MARSS

dfa_1 <- MARSS(y = dat, model = mod_list, inits = init_list, 
               control = con_list)

library(dplyr)
library(writexl)
library(RODBC)

'import Tableau saved file'
path = file.path("C:/test/export_material_currency.mdb")
conn <- odbcConnectAccess(path) 
RODBC::sqlTables(conn)
import1 <- sqlFetch(conn, "DATA") 
odbcClose(conn)

names(import1)[names(import1) == "Partner clean"] <- "Partner"
names(import1)[names(import1) == "Product Family"] <- "Family"
names(import1)[names(import1) == "Material Product"] <- "Product"
names(import1)[names(import1) == "Material Sub Product"] <- "Subproduct"
names(import1)[names(import1) == "COPA Material"] <- "COPA"
names(import1)[names(import1) == "SD Document currency"] <- "Currency"
names(import1)[names(import1) == "Cost of sales (SUM)"] <- "COGS"
names(import1)[names(import1) == "Quantity (SUM)"] <- "Quantity"
names(import1)[names(import1) == "Sales value (SUM)"] <- "Sales"
names(import1)[names(import1) == "Logistics revenue (SUM)"] <- "Logistics"
names(import1)[names(import1) == "SD exchange rate (AVG)"] <- "M_rate"
names(import1)[names(import1) == "S02 exchange rate (AVG)"] <- "S02_rate"
names(import1)[names(import1) == "Parts Discount (SUM)"] <- "Parts_discount"
names(import1)[names(import1) == "Logistics Discount (SUM)"] <- "Logistics_discount"
names(import1)[names(import1) == "overheads (SUM)"] <- "overheads"

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
df = import1
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Interco", Currency == "CHF", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Interco", Currency == "CHF", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_CHF = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Interco", Currency == "EUR", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Interco", Currency == "EUR", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_EUR = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Interco", Currency == "USD", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Interco", Currency == "USD", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_USD = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Interco", Currency == "CNY", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Interco", Currency == "CNY", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_CNY = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

df_outer <- rbind(df_outer_CHF, df_outer_EUR, df_outer_USD, df_outer_CNY)

df = import1
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Third", Currency == "CHF", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Third", Currency == "CHF", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_CHF_third = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Third", Currency == "EUR", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Third", Currency == "EUR", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_EUR_third = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Third", Currency == "USD", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Third", Currency == "USD", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_USD_third = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

'populate data frame and join ref1 & ref1 within one data frame FOR ONE CURRENCY'
ref1_vector <- df %>% filter(Ref1 == "Ref1", Partner == "Third", Currency == "CNY", .preserve = FALSE)
ref2_vector <- df %>% filter(Ref1 == "Ref2", Partner == "Third", Currency == "CNY", .preserve = FALSE)
df_inner = ref1_vector %>% inner_join(ref2_vector, by="COPA")
df_outer_CNY_third = merge(x=ref1_vector, y=ref2_vector, by="COPA", all=TRUE)

df_outer_third <- rbind(df_outer_CHF_third, df_outer_EUR_third, df_outer_USD_third, df_outer_CNY_third)
df_outer <- rbind(df_outer, df_outer_third)
View(df_outer)

'replace all NA values with 0'
df_outer$Ref1.x[is.na(df_outer$Ref1.x)]<-0
df_outer$Ref1.x[]<- "Ref1"
df_outer$COGS.x[is.na(df_outer$COGS.x)]<-0
df_outer$Quantity.x[is.na(df_outer$Quantity.x)]<-0
df_outer$Sales.x[is.na(df_outer$Sales.x)]<-0
df_outer$Sales.y[is.na(df_outer$Sales.y)]<-0
df_outer$Logistics.x[is.na(df_outer$Logistics.x)]<-0
df_outer$Logistics.y[is.na(df_outer$Logistics.y)]<-0
df_outer$S02_rate.x[is.na(df_outer$Sales.x)]<-0
df_outer$S02_rate.y[is.na(df_outer$Sales.y)]<-0
df_outer$M_rate.x[is.na(df_outer$Sales_S02.y)]<-0
df_outer$M_rate.y[is.na(df_outer$Sales_S02.y)]<-0
df_outer$Quantity.y[is.na(df_outer$Quantity.y)]<-0
df_outer$COGS.y[is.na(df_outer$COGS.y)]<-0
df_outer$Parts_discount.x[is.na(df_outer$Parts_discount.x)]<-0
df_outer$Parts_discount.y[is.na(df_outer$Parts_discount.y)]<-0
df_outer$Logistics_discount.x[is.na(df_outer$Logistics_discount.x)]<-0
df_outer$Logistics_discount.y[is.na(df_outer$Logistics_discount.y)]<-0
df_outer$Ref1.y[]<- "Ref2"
df_outer$overheads.x[is.na(df_outer$overheads.x)]<-0
df_outer$overheads.y[is.na(df_outer$overheads.y)]<-0

'replace all blank values with 0'
df_outer$Ref1.x[is.nan(df_outer$Ref1.x)]<-0
df_outer$COGS.x[is.nan(df_outer$COGS.x)]<-0
df_outer$Quantity.x[is.nan(df_outer$Quantity.x)]<-0
df_outer$Sales.x[is.nan(df_outer$Sales.x)]<-0
df_outer$Sales.y[is.nan]<-0
df_outer$Logistics.x[is.nan(df_outer$Logistics.x)]<-0
df_outer$Logistics.y[is.nan(df_outer$Logistics.y)]<-0
df_outer$S02_rate.x[is.nan(df_outer$Sales.x)]<-0
df_outer$S02_rate.y[is.nan(df_outer$Sales.y)]<-0
df_outer$M_rate.x[is.nan(df_outer$Sales_S02.y)]<-0
df_outer$M_rate.y[is.nan(df_outer$Sales_S02.y)]<-0
df_outer$Quantity.y[is.nan(df_outer$Quantity.y)]<-0
df_outer$COGS.y[is.nan(df_outer$COGS.y)]<-0
df_outer$Parts_discount.x[is.nan(df_outer$Parts_discount.x)]<-0
df_outer$Parts_discount.y[is.nan(df_outer$Parts_discount.y)]<-0
df_outer$Logistics_discount.x[is.nan(df_outer$Logistics_discount.x)]<-0
df_outer$Logistics_discount.y[is.nan(df_outer$Logistics_discount.y)]<-0
df_outer$overheads.x[is.nan(df_outer$overheads.x)]<-0
df_outer$overheads.y[is.nan(df_outer$overheads.y)]<-0


'enhance missing ref1 ref2 data'
df_outer$Subproduct.y[is.na(df_outer$Subproduct.y)] <- df_outer$Subproduct.x[is.na(df_outer$Subproduct.y)]
df_outer$Family.y[is.na(df_outer$Family.y)] <- df_outer$Family.x[is.na(df_outer$Family.y)]
df_outer$Partner.y[is.na(df_outer$Partner.y)] <- df_outer$Partner.x[is.na(df_outer$Partner.y)]
df_outer$Subproduct.x[is.na(df_outer$Subproduct.x)] <- df_outer$Subproduct.y[is.na(df_outer$Subproduct.x)]
df_outer$Family.x[is.na(df_outer$Family.x)] <- df_outer$Family.y[is.na(df_outer$Family.x)]
df_outer$Partner.x[is.na(df_outer$Partner.x)] <- df_outer$Partner.y[is.na(df_outer$Partner.x)]
View(df_outer)

'calculate unit prices and variations'
df_outer$delta_qty <- (df_outer$Quantity.y - df_outer$Quantity.x)
df_outer$delta_sales <- (df_outer$Sales.y - df_outer$Sales.x)
df_outer$delta_M_rate <- (df_outer$M_rate.y - df_outer$M_rate.x)
df_outer$delta_S02_rate <- (df_outer$S02_rate.y - df_outer$S02_rate.x)
df_outer$delta_COGS <- (df_outer$COGS.y - df_outer$COGS.x)
df_outer$unit_price.x <- (df_outer$Sales.x / df_outer$Quantity.x)
df_outer$delta_M_rate_percent <- (df_outer$delta_M_rate / df_outer$M_rate.x)
df_outer$delta_S02_rate_percent <- (df_outer$delta_S02_rate / df_outer$S02_rate.x)
df_outer$unit_price.y <- (df_outer$Sales.y / df_outer$Quantity.y)
df_outer$unit_COGS.x <- (df_outer$COGS.x / df_outer$Quantity.x)
df_outer$delta_unit_sales <- (df_outer$unit_price.y - df_outer$unit_price.x)
df_outer$unit_parts_discount.x <- (df_outer$Parts_discount.x / df_outer$Quantity.x)
df_outer$unit_parts_discount.y <- (df_outer$Parts_discount.y / df_outer$Quantity.y)
df_outer$delta_unit_parts_discount <- (df_outer$unit_parts_discount.y - df_outer$unit_parts_discount.x)
df_outer$unit_logistics_discount.x <- (df_outer$Logistics_discount.x / df_outer$Quantity.x)
df_outer$unit_logistics_discount.y <- (df_outer$Logistics_discount.y / df_outer$Quantity.y)
df_outer$delta_unit_logistics_discount <- (df_outer$unit_logistics_discount.y - df_outer$unit_logistics_discount.x)
df_outer$unit_logistics.x <- (df_outer$Logistics.x / df_outer$Quantity.x)
df_outer$unit_logistics.y <- (df_outer$Logistics.y / df_outer$Quantity.y)
df_outer$delta_unit_logistics <- (df_outer$unit_logistics.y - df_outer$unit_logistics.x)
'test for absurd discounts'
df_outer$parts_discount_test.x <- df_outer$unit_price.x + df_outer$unit_parts_discount.x
df_outer$parts_discount_test.y <- df_outer$unit_price.y + df_outer$unit_parts_discount.y
df_outer$delta_unit_parts_discount[df_outer$parts_discount_test.x<0] <- 0
df_outer$delta_unit_parts_discount[df_outer$parts_discount_test.y<0] <- 0
df_outer$logistics_discount_test.x <- df_outer$unit_price.x + df_outer$unit_logistics_discount.x
df_outer$logistics_discount_test.y <- df_outer$unit_price.y + df_outer$unit_logistics_discount.y
df_outer$delta_unit_logistics_discount[df_outer$logistics_discount_test.x<0] <- 0
df_outer$delta_unit_logistics_discount[df_outer$logistics_discount_test.y<0] <- 0


df_outer$unit_price.x[is.infinite(df_outer$unit_price.x)] <- 0
df_outer$unit_price.y[is.infinite(df_outer$unit_price.y)] <- 0

'calculate volume effect'
df_outer$Volume_effect <- (df_outer$delta_qty * (df_outer$unit_price.x - df_outer$unit_COGS.x))

'calculate FX effect based on M FX rate'
df_outer$Price_effect <- (df_outer$Quantity.x * df_outer$delta_unit_sales + df_outer$delta_qty * df_outer$delta_unit_sales)
df_outer$FX_effect <- ( df_outer$delta_M_rate_percent * df_outer$Sales.x + df_outer$delta_qty * df_outer$delta_M_rate_percent *df_outer$unit_price.x)
df_outer$S02_effect <- -( df_outer$delta_S02_rate_percent * df_outer$Sales.x + df_outer$delta_qty * df_outer$delta_S02_rate_percent *df_outer$unit_price.x)
df_outer$Price_effect <- df_outer$Price_effect - df_outer$FX_effect - df_outer$S02_effect 

'calculate discount effect'
df_outer$Parts_discount_effect <- (df_outer$delta_unit_parts_discount  * df_outer$Quantity.x + df_outer$delta_qty * df_outer$delta_unit_parts_discount)
df_outer$Price_effect <- df_outer$Price_effect - df_outer$Parts_discount_effect
df_outer$Logistics_discount_effect <- (df_outer$delta_unit_logistics_discount  * df_outer$Quantity.x + df_outer$delta_qty * df_outer$delta_unit_logistics_discount)
df_outer$Price_effect <- df_outer$Price_effect - df_outer$Logistics_discount_effect

'calculate logistics effect'
df_outer$Logistics_effect <- (df_outer$delta_unit_logistics  * df_outer$Quantity.x + df_outer$delta_qty * df_outer$delta_unit_logistics)
df_outer$Price_effect <- df_outer$Price_effect - df_outer$Logistics_effect


'calculate COGS effect'

df_outer$unit_COGS.y <- (df_outer$COGS.y / df_outer$Quantity.y)
df_outer$delta_unit_COGS <- (df_outer$unit_COGS.y - df_outer$unit_COGS.x)
df_outer$COGS_effect <- (-(df_outer$Quantity.x * df_outer$delta_unit_COGS) - df_outer$delta_qty * df_outer$delta_unit_COGS)


'calculate overheads effect'
df_outer$overheads_effect <- df_outer$overheads.y - df_outer$overheads.x
df_outer$Mix_effect <- (is.nan(df_outer$unit_price.x))*(df_outer$Sales.y - df_outer$COGS.y)-(is.nan(df_outer$unit_price.y))*(df_outer$Sales.x - df_outer$COGS.x)
df_outer$Mix_effect[is.na(df_outer$overheads.y)] <- df_outer$Mix_effect[is.na(df_outer$overheads.y)] + df_outer$overheads.x[is.na(df_outer$overheads.y)]
df_outer$Mix_effect[is.na(df_outer$overheads.x)] <- df_outer$Mix_effect[is.na(df_outer$overheads.x)] - df_outer$overheads.y[is.na(df_outer$overheads.x)]

df_outer$overheads_effect[is.na(df_outer$overheads_effect)]<-  0




'replaces nulls with 0'
df_outer$Volume_effect[is.nan(df_outer$Volume_effect)]<- 0
df_outer$Price_effect[is.nan(df_outer$Price_effect)]<- 0
df_outer$COGS_effect[is.nan(df_outer$COGS_effect)]<- 0
df_outer$Volume_effect[is.nan(df_outer$delta_unit_sales)]<- 0
df_outer$FX_effect[is.nan(df_outer$FX_effect)]<- 0
df_outer$S02_effect[is.nan(df_outer$S02_effect)]<- 0
df_outer$FX_effect[is.na(df_outer$FX_effect)]<- 0
df_outer$S02_effect[is.na(df_outer$S02_effect)]<- 0
df_outer$Parts_discount_effect[is.na(df_outer$Parts_discount_effect)]<- 0
df_outer$Logistics_discount_effect[is.na(df_outer$Logistics_discount_effect)]<- 0
df_outer$Logistics_effect[is.na(df_outer$Logistics_effect)]<- 0
df_outer$overheads_effect[is.na(df_outer$overheads_effect)]<- 0


df_outer$unit_price.x_restated_discount <-   df_outer$unit_parts_discount.x 
df_outer$unit_price.y_restated_discount <-  - df_outer$unit_parts_discount.y 
df_outer$delta_unit_price_restated_discount <- (df_outer$unit_price.y_restated_discount - df_outer$unit_price.x_restated_discount)

df_outer$unit_price.x_restated_logistics <-   df_outer$unit_logistics.x + df_outer$unit_logistics_discount.x 
df_outer$unit_price.y_restated_logistics <- - (df_outer$unit_logistics.y + df_outer$unit_logistics_discount.y)
df_outer$delta_unit_price_restated_logistics <- (df_outer$unit_price.y_restated_logistics - df_outer$unit_price.x_restated_logistics)

df_outer$unit_price.x_restated_fx <- 0
df_outer$unit_price.y_restated_fx <-   - (df_outer$FX_effect + df_outer$S02_effect)/df_outer$Quantity.y
df_outer$delta_unit_price_restated_fx <- (df_outer$unit_price.y_restated_fx - df_outer$unit_price.x_restated_fx)


df_outer$unit_price.x_restated <- df_outer$unit_price.x - df_outer$unit_parts_discount.x - df_outer$unit_logistics.x - df_outer$unit_logistics_discount.x 
df_outer$unit_price.y_restated <- df_outer$unit_price.y - df_outer$unit_parts_discount.y - df_outer$unit_logistics.y - df_outer$unit_logistics_discount.y - (df_outer$FX_effect + df_outer$S02_effect)/df_outer$Quantity.y
df_outer$delta_unit_price_restated <- (df_outer$unit_price.y_restated - df_outer$unit_price.x_restated)


View(df_outer)
df <- df_outer


'further fine-tune manual subproduct category'

df$Subproduct <- with(df,is.na(df$Subproduct.y)  & df$Quantity.y>999)
df$Subproduct[ df$Subproduct == "TRUE" ] <- "High quantity product"
df$Subproduct[ df$Subproduct == "FALSE" ] <- df$Subproduct.y[df$Subproduct == "FALSE"]
df$Subproduct[ df$Subproduct == "FALSE" ] <- NA
df$Subproduct.y <- df$Subproduct
df$Subproduct <- with(df,is.na(df$Subproduct.y)  & df$Quantity.y<999 & df$Quantity.y>99 )
df$Subproduct[ df$Subproduct == "TRUE" ] <- "Medium quantity product"
df$Subproduct[ df$Subproduct == "FALSE" ] <- df$Subproduct.y[df$Subproduct == "FALSE"]
df$Subproduct[ df$Subproduct == "FALSE" ] <- NA
df$Subproduct.y <- df$Subproduct
df$Subproduct <- with(df,is.na(df$Subproduct.y)  & df$Quantity.y<99 & df$unit_price.y > 5000 )
df$Subproduct[ df$Subproduct == "TRUE" ] <- "High value product"
df[df$Subproduct == "FALSE" & !is.na(df$Subproduct.y), ]$Subproduct <- df$Subproduct.y[df$Subproduct == "FALSE" & !is.na(df$Subproduct.y) ]
df$Subproduct[ df$Subproduct == "FALSE" ] <- NA
                          


View(df)

Partner <- df$Partner.y
Family <- df$Family.y
Product <- df$Product.y
Subproduct <- df$Subproduct
COPA <- df$COPA
Currency <- df$Currency.x
Year <- 2022
vari <- ""

Quantity <- df$Quantity.x
Sales <- df$Sales.x
COGS <- df$COGS.x
overheads <- df$overheads.x
Margin <- Sales - COGS - overheads
Margin_2022 <- Margin
unit_price <- df$unit_price.x
unit_price_restated_discount <- df$unit_price.x_restated_discount
unit_price_restated_logistics <- df$unit_price.x_restated_logistics
unit_price_restated_fx <- df$unit_price.x_restated_fx
unit_price_restated <- df$unit_price.x_restated
unit_COGS <- df$unit_COGS.x

delta_unit_price <- "placeholder"
delta_unit_price_restated_discount <- "placeholder"
delta_unit_price_restated_logistics <- "placeholder"
delta_unit_price_restated_fx <- "placeholder"
delta_unit_price_restated <- "placeholder"
delta_M_rate_percent <- "placeholder"
delta_S02_rate_percent <- "placeholder"
delta_parts_discount <- "placeholder"
delta_logistics_discount <- "placeholder"
delta_logistics <- "placeholder"
delta_COGS <- "placeholder"
Calc1 <- "placeholder"
Delta_unit_price_percent <- "placeholder"
Delta_unit_price_restated_discount_percent <- "placeholder"
Delta_unit_price_restated_logistics_percent <- "placeholder"
Delta_unit_price_restated_fx_percent <- "placeholder"
Delta_unit_price_restated_percent <- "placeholder"
Calc2 <- "placeholder"
Calc3 <- "placeholder"
Delta_COGS_percent <- "placeholder"
Calc4 <- "placeholder"
Calc5 <- "placeholder"
Delta_qty_percent <- "placeholder"
Volume <- "placeholder"
Price <- "placeholder"
Logistics <- "placeholder"
Parts_discount <- "placeholder"
Logistics_discount <- "placeholder"
S02 <- "placeholder"
FX <- "placeholder"
Cost <- "placeholder"
Mix <- "placeholder"
MarginQuarter <- "placeholder"
Lignecount <- "placeholder"

df_2022 <- data.frame(Partner, Family, Product, Subproduct, COPA, Currency,  Year, vari, Quantity, Sales, COGS, Margin, overheads, unit_price, unit_price_restated_discount, unit_price_restated_logistics, unit_price_restated_fx, unit_price_restated, unit_COGS, delta_unit_price, delta_unit_price_restated_discount, delta_unit_price_restated_logistics, delta_unit_price_restated_fx, delta_unit_price_restated, delta_M_rate_percent, delta_S02_rate_percent, delta_parts_discount, delta_logistics_discount, delta_COGS, Calc1,Delta_unit_price_percent, Delta_unit_price_restated_discount_percent,Delta_unit_price_restated_logistics_percent, Delta_unit_price_restated_fx_percent, Delta_unit_price_restated_percent, Calc2, Calc3, Delta_COGS_percent, Calc4, Calc5, Delta_qty_percent, Volume, Price, Logistics, Parts_discount, Logistics_discount, S02, FX, Cost, Mix, MarginQuarter, Lignecount)

Year <- 2023
Currency <- df$Currency.y
Quantity <- df$Quantity.y
Sales <- df$Sales.y
COGS <- df$COGS.y
overheads <- df$overheads.y
Margin <- Sales - COGS - overheads
overheads <- df$overheads.y
Margin_2023 <- Margin
unit_price <- df$unit_price.y
unit_price_restated_discount <- df$unit_price.y_restated_discount
unit_price_restated_logitics <- df$unit_price.y_restated_logistics
unit_price_restated_fx <- df$unit_price.y_restated_fx
unit_price_restated <- df$unit_price.y_restated
unit_COGS <- df$unit_COGS.y

df_2023 <- data.frame(Partner, Family, Product, Subproduct, COPA, Currency,  Year, vari, Quantity, Sales, COGS, Margin, overheads, unit_price, unit_price_restated_discount, unit_price_restated_logistics, unit_price_restated_fx, unit_price_restated, unit_COGS, delta_unit_price, delta_unit_price_restated_discount, delta_unit_price_restated_logistics, delta_unit_price_restated_fx, delta_unit_price_restated,delta_M_rate_percent, delta_S02_rate_percent, delta_parts_discount, delta_logistics_discount, delta_COGS, Calc1,Delta_unit_price_percent, Delta_unit_price_restated_discount_percent, Delta_unit_price_restated_logistics_percent, Delta_unit_price_restated_fx_percent, Delta_unit_price_restated_percent, Calc2, Calc3, Delta_COGS_percent, Calc4, Calc5, Delta_qty_percent, Volume, Price, Logistics, Parts_discount, Logistics_discount, S02, FX, Cost, Mix, MarginQuarter, Lignecount)
df_bridge <- rbind(df_2022, df_2023)


vari <- "delta"

Currency[is.na(Currency)] <- df$Currency.x[is.na(Currency)]
Quantity <- "placeholder"
Sales <- "placeholder"
COGS <- "placeholder"
Margin <- "placeholder"
unit_price <- "placeholder"
unit_price_restated_discount <- "placeholder"
unit_price_restated_logistics <- "placeholder"
unit_price_restated_fx <- "placeholder"
unit_price_restated <- "placeholder"
unit_COGS <- "placeholder"
overheads <- "placeholder"


delta_unit_price <- df$delta_unit_sales
delta_unit_price_restated_discount <- df$delta_unit_price_restated_discount
delta_unit_price_restated_logistics <- df$delta_unit_price_restated_logistics
delta_unit_price_restated_fx <- df$delta_unit_price_restated_fx
delta_unit_price_restated <- df$delta_unit_price_restated
delta_M_rate_percent <- df$delta_M_rate_percent
delta_S02_rate_percent <- df$delta_S02_rate_percent
delta_parts_discount <- df$delta_unit_parts_discount
delta_logistics_discount <- df$delta_unit_logistics_discount
delta_COGS <- df$delta_unit_COGS
Calc1 <- df$unit_price.x
Delta_unit_price_percent <- delta_unit_price / Calc1
Delta_unit_price_restated_discount_percent <- delta_unit_price_restated_discount / Calc1
Delta_unit_price_restated_logistics_percent <- delta_unit_price_restated_logistics / Calc1
Delta_unit_price_restated_fx_percent <- delta_unit_price_restated_fx / Calc1
Delta_unit_price_restated_percent <- delta_unit_price_restated / Calc1
Calc2 <- df$unit_COGS.x
Calc3 <- df$Sales.y
Delta_COGS_percent <- delta_COGS / Calc2
Calc4 <- df$delta_qty
Calc5 <- df$Quantity.x
Delta_qty_percent <- Calc4 / Calc5
Volume <- df$Volume_effect - df$overheads_effect
Price <- df$Price_effect
Logistics <- df$Logistics_effect
Parts_discount <-df$Parts_discount_effect
Logistics_discount <-df$Logistics_discount_effect
S02 <- df$S02_effect
Cost <- df$COGS_effect
FX <- df$FX_effect
Mix <- df$Mix_effect
MarginQuarter <- Margin_2023
Lignecount <- 1

df_temp <- data.frame(Partner, Family, Product, Subproduct, COPA, Currency,  Year, vari, Quantity, Sales, COGS, Margin, overheads, unit_price, unit_price_restated_discount, unit_price_restated_logistics, unit_price_restated_fx, unit_price_restated, unit_COGS, delta_unit_price, delta_unit_price_restated_discount, delta_unit_price_restated_logistics, delta_unit_price_restated_fx, delta_unit_price_restated, delta_M_rate_percent, delta_S02_rate_percent, delta_parts_discount, delta_logistics_discount, delta_COGS, Calc1,Delta_unit_price_percent,Delta_unit_price_restated_discount_percent, Delta_unit_price_restated_logistics_percent, Delta_unit_price_restated_fx_percent, Delta_unit_price_restated_percent, Calc2, Calc3, Delta_COGS_percent, Calc4, Calc5, Delta_qty_percent, Volume, Price, Logistics, Parts_discount, Logistics_discount, S02, FX, Cost, Mix, MarginQuarter, Lignecount)
df_bridge_margin_2023 <- rbind(df_bridge, df_temp)



Year <- 2022
Currency <- df$Currency.x
delta_unit_price <- "placeholder_0"
delta_unit_price_restated <- "placeholder_0"
delta_COGS <- "placeholder_0"
Calc1 <- "placeholder_0"
Delta_unit_price_percent <- "placeholder_0"
Delta_unit_price_restated_discount_percent <- "placeholder_0"
Delta_unit_price_restated_logistics_percent <- "placeholder_0"
Delta_unit_price_restated_fx_percent <- "placeholder_0"
Delta_unit_price_restated_percent <- "placeholder_0"
delta_M_rate_percent <- "placeholder_0"
delta_S02_rate_percent <- "placeholder_0"
delta_parts_discount <- "placeholder"
delta_logistics_discount <- "placeholder"
delta_logistics <- "placeholder"
Calc2 <- "placeholder_0"
Calc3 <- "placeholder_0"
Delta_COGS_percent <- "placeholder_0"
Calc4 <- "placeholder_0"
Calc5 <- "placeholder_0"
Delta_qty_percent <- "placeholder_0"
Volume <- "placeholder_0"
Price <- "placeholder_0"
Logistics <- "placeholder_0"
Parts_discount <- "placeholder_0"
Logistics_discount <- "placeholder_0"
S02 <- "placeholder_0"
FX <- "placeholder_0"
Cost <- "placeholder_0"
Mix <- "placeholder_0"
MarginQuarter <- Margin_2022
Lignecount <- 1

df_temp <- data.frame(Partner, Family, Product, Subproduct, COPA, Currency,  Year, vari, Quantity, Sales, COGS, Margin, overheads, unit_price, unit_price_restated_discount,unit_price_restated_logistics,unit_price_restated_fx,unit_price_restated, unit_COGS, delta_unit_price,delta_unit_price_restated_discount,delta_unit_price_restated_logistics,delta_unit_price_restated_fx,delta_unit_price_restated, delta_M_rate_percent, delta_S02_rate_percent, delta_parts_discount, delta_logistics_discount, delta_COGS, Calc1,Delta_unit_price_percent,Delta_unit_price_restated_discount_percent ,Delta_unit_price_restated_logistics_percent ,Delta_unit_price_restated_fx_percent ,Delta_unit_price_restated_percent, Calc2, Calc3, Delta_COGS_percent, Calc4, Calc5, Delta_qty_percent, Volume, Price, Logistics, Parts_discount, Logistics_discount, S02, FX, Cost, Mix, MarginQuarter, Lignecount)
df_bridge_margin <- rbind(df_bridge_margin_2023, df_temp)


df_bridge_margin[ df_bridge_margin == "placeholder" ] <- ""
df_bridge_margin[ df_bridge_margin == "placeholder_0" ] <- 0
df_bridge_margin$delta_unit_price[is.na(df_bridge_margin$delta_unit_price)]<-0
df_bridge_margin$delta_unit_price[is.nan(df_bridge_margin$delta_unit_price)]<-0
df_bridge_margin$delta_unit_price_restated_discount[is.na(df_bridge_margin$delta_unit_price_restated_discount)]<-0
df_bridge_margin$delta_unit_price_restated_discount[is.nan(df_bridge_margin$delta_unit_price_restated_discount)]<-0
df_bridge_margin$delta_unit_price_restated_logistics[is.na(df_bridge_margin$delta_unit_price_restated_logistics)]<-0
df_bridge_margin$delta_unit_price_restated_logistics[is.nan(df_bridge_margin$delta_unit_price_restated_logistics)]<-0
df_bridge_margin$delta_unit_price_restated_fx[is.na(df_bridge_margin$delta_unit_price_restated_fx)]<-0
df_bridge_margin$delta_unit_price_restated_fx[is.nan(df_bridge_margin$delta_unit_price_restated_fx)]<-0
df_bridge_margin$delta_unit_price_restated[is.na(df_bridge_margin$delta_unit_price_restated)]<-0
df_bridge_margin$delta_unit_price_restated[is.nan(df_bridge_margin$delta_unit_price_restated)]<-0
df_bridge_margin$delta_COGS[is.nan(df_bridge_margin$delta_COGS)]<-0
df_bridge_margin$delta_COGS[is.na(df_bridge_margin$delta_COGS)]<-0
df_bridge_margin$Calc1[is.na(df_bridge_margin$Calc1)]<-0
df_bridge_margin$Calc2[is.na(df_bridge_margin$Calc2)]<-0
df_bridge_margin$Calc3[is.na(df_bridge_margin$Calc3)]<-0
df_bridge_margin$Calc4[is.na(df_bridge_margin$Calc4)]<-0
df_bridge_margin$Calc5[is.na(df_bridge_margin$Calc5)]<-0
df_bridge_margin$Delta_COGS_percent[is.na(df_bridge_margin$Delta_COGS_percent)]<-0
df_bridge_margin$Delta_qty_percent[is.na(df_bridge_margin$Delta_qty_percent)]<-0
df_bridge_margin$Delta_unit_price_percent[is.na(df_bridge_margin$Delta_unit_price_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_discount_percent[is.na(df_bridge_margin$Delta_unit_price_restated_discount_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_logistics_percent[is.na(df_bridge_margin$Delta_unit_price_restated_logistics_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_fx_percent[is.na(df_bridge_margin$Delta_unit_price_restated_fx_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_percent[is.na(df_bridge_margin$Delta_unit_price_restated_percent)]<-0
df_bridge_margin$delta_M_rate_percent[is.na(df_bridge_margin$delta_M_rate_percent)]<-0
df_bridge_margin$delta_S02_rate_percent[is.na(df_bridge_margin$delta_S02_rate_percent)]<-0
df_bridge_margin$Calc1[is.nan(df_bridge_margin$Calc1)]<-0
df_bridge_margin$Calc2[is.nan(df_bridge_margin$Calc2)]<-0
df_bridge_margin$Calc3[is.nan(df_bridge_margin$Calc3)]<-0
df_bridge_margin$Calc4[is.nan(df_bridge_margin$Calc4)]<-0
df_bridge_margin$Calc5[is.nan(df_bridge_margin$Calc5)]<-0
df_bridge_margin$Delta_COGS_percent[is.nan(df_bridge_margin$Delta_COGS_percent)]<-0
df_bridge_margin$Delta_qty_percent[is.nan(df_bridge_margin$Delta_qty_percent)]<-0
df_bridge_margin$Delta_unit_price_percent[is.nan(df_bridge_margin$Delta_unit_price_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_discount_percent[is.nan(df_bridge_margin$Delta_unit_price_restated_discount_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_logistics_percent[is.nan(df_bridge_margin$Delta_unit_price_restated_logistics_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_fx_percent[is.nan(df_bridge_margin$Delta_unit_price_restated_fx_percent)]<-0
df_bridge_margin$Delta_unit_price_restated_percent[is.nan(df_bridge_margin$Delta_unit_price_restated_percent)]<-0
df_bridge_margin$delta_M_rate_percent[is.nan(df_bridge_margin$delta_M_rate_percent)]<-0
df_bridge_margin$delta_S02_rate_percent[is.nan(df_bridge_margin$delta_S02_rate_percent)]<-0
df_bridge_margin$Price[is.nan(df_bridge_margin$Price)]<-0
df_bridge_margin$S02[is.nan(df_bridge_margin$S02)]<-0
df_bridge_margin$FX[is.nan(df_bridge_margin$FX)]<-0
df_bridge_margin$Price[is.na(df_bridge_margin$Price)]<-0
df_bridge_margin$S02[is.na(df_bridge_margin$S02)]<-0
df_bridge_margin$FX[is.na(df_bridge_margin$FX)]<-0

df_bridge_margin$Quantity   <- as.numeric(unlist(df_bridge_margin$Quantity))
df_bridge_margin$Sales   <- as.numeric(unlist(df_bridge_margin$Sales))
df_bridge_margin$COGS   <- as.numeric(unlist(df_bridge_margin$COGS))
df_bridge_margin$Margin   <- as.numeric(unlist(df_bridge_margin$Margin))
df_bridge_margin$unit_price   <- as.numeric(unlist(df_bridge_margin$unit_price))
df_bridge_margin$unit_price_restated_discount   <- as.numeric(unlist(df_bridge_margin$unit_price_restated_discount))
df_bridge_margin$unit_price_restated_logistics   <- as.numeric(unlist(df_bridge_margin$unit_price_restated_logistics))
df_bridge_margin$unit_price_restated_fx   <- as.numeric(unlist(df_bridge_margin$unit_price_restated_fx))
df_bridge_margin$unit_price_restated   <- as.numeric(unlist(df_bridge_margin$unit_price_restated))
df_bridge_margin$unit_COGS  <- as.numeric(unlist(df_bridge_margin$unit_COGS))
df_bridge_margin$delta_unit_price  <- as.numeric(unlist(df_bridge_margin$delta_unit_price))
df_bridge_margin$delta_M_rate_percent <- as.numeric(unlist(df_bridge_margin$delta_M_rate_percent))
df_bridge_margin$delta_S02_rate_percent <- as.numeric(unlist(df_bridge_margin$delta_S02_rate_percent))
df_bridge_margin$delta_parts_discount <- as.numeric(unlist(df_bridge_margin$delta_parts_discount))
df_bridge_margin$delta_logistics_discount <- as.numeric(unlist(df_bridge_margin$delta_logistics_discount))
df_bridge_margin$delta_COGS  <- as.numeric(unlist(df_bridge_margin$delta_COGS))
df_bridge_margin$Calc1  <- as.numeric(unlist(df_bridge_margin$Calc1))
df_bridge_margin$Calc2  <- as.numeric(unlist(df_bridge_margin$Calc2))
df_bridge_margin$Calc3  <- as.numeric(unlist(df_bridge_margin$Calc3))
df_bridge_margin$Calc4  <- as.numeric(unlist(df_bridge_margin$Calc4))
df_bridge_margin$Calc5  <- as.numeric(unlist(df_bridge_margin$Calc5))
df_bridge_margin$Delta_COGS_percent <- as.numeric(unlist(df_bridge_margin$Delta_COGS_percent))
df_bridge_margin$Delta_qty_percent <- as.numeric(unlist(df_bridge_margin$Delta_qty_percent))
df_bridge_margin$Delta_unit_price_percent <- as.numeric(unlist(df_bridge_margin$Delta_unit_price_percent))
df_bridge_margin$Delta_unit_price_restated_discount_percent <- as.numeric(unlist(df_bridge_margin$Delta_unit_price_restated_discount_percent))
df_bridge_margin$Delta_unit_price_restated_logistics_percent <- as.numeric(unlist(df_bridge_margin$Delta_unit_price_restated_logistics_percent))
df_bridge_margin$Delta_unit_price_restated_fx_percent <- as.numeric(unlist(df_bridge_margin$Delta_unit_price_restated_fx_percent))
df_bridge_margin$Delta_unit_price_restated_percent <- as.numeric(unlist(df_bridge_margin$Delta_unit_price_restated_percent))
df_bridge_margin$MarginQuarter   <- as.numeric(unlist(df_bridge_margin$MarginQuarter))
df_bridge_margin$Volume   <- as.numeric(unlist(df_bridge_margin$Volume))
df_bridge_margin$Price  <- as.numeric(unlist(df_bridge_margin$Price))
df_bridge_margin$S02  <- as.numeric(unlist(df_bridge_margin$S02))
df_bridge_margin$Logistics  <- as.numeric(unlist(df_bridge_margin$Logistics))
df_bridge_margin$Parts_discount  <- as.numeric(unlist(df_bridge_margin$Parts_discount))
df_bridge_margin$Logistics_discount  <- as.numeric(unlist(df_bridge_margin$Logistics_discount))
df_bridge_margin$FX  <- as.numeric(unlist(df_bridge_margin$FX))
df_bridge_margin$Cost   <- as.numeric(unlist(df_bridge_margin$Cost))
df_bridge_margin$Mix   <- as.numeric(unlist(df_bridge_margin$Mix))
df_bridge_margin$Lignecount   <- as.numeric(unlist(df_bridge_margin$Lignecount))
df_bridge_margin$overheads   <- as.numeric(unlist(df_bridge_margin$overheads))


View(df_bridge_margin)

write_xlsx(df_bridge_margin, "C:/test/VPM_result_bridge_material.xlsx")

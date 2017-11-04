
library(SparkR)
#head

headf <- function(test){
    showDF(test, numRows = 20, truncate = TRUE)
}

#Load data

raw <- get_dataset("/Unilever/Private/Acxiom/dataset/Attribute With Demo - 2016 11-11 Active Customers")
class(raw)
headf(raw)

#Test data

test <- select(raw,
    'user_hash'
    ,'bought_衣物清洁'
    ,'bought_衣物护理'
    ,'bought_洗发'
    ,'bought_沐浴'
    ,'bought_护发'
    ,'P1M_order_num_third_cate'
    ,'P1M_order_num_brand'
    ,'P1M_order_num_days_active'
    ,'P1M_order_avg_ticket_size'
    ,'P1M_order_avg_discount_rate'
    ,'P1M_order_CPG'
    ,'jd_user_level'
    ,'gender'
    ,'age'
    ,'marital_status'
    ,'education'
    ,'profession'
    ,'province'
    ,'city'
    ,'has_child'
    ,'has_car'
    ,'purchasing_power'
    ,'payment_method'
    ,'promo_sensitivity'
    ,'browser_client'
)

#sc <- sparkR.init()
#sqlContext <- sparkRSQL.init(sc)
registerTempTable(test, "table")
test <- sql(sqlContext, "SELECT * FROM table limit 1000")
dim(test)
headf(test)


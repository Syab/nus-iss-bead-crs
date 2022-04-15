function dateutil() {
    this.addXSeconds = function (dateString){
        d = new Date(dateString)
        d.setSeconds(d.getSeconds()+ (60 - d.getSeconds()));
        d.setHours(d.getUTCHours()+ 8);

        return(
            d.getFullYear()
            +"-"+
            ("0" + (d.getMonth() + 1)).slice(-2)
            +"-"+
            ("0" + d.getDate()).slice(-2)
            +"T"+
            ("0" + d.getHours()).slice(-2)
            +":"+
            ("0" + d.getMinutes()).slice(-2)
            +":"+
            ("0" + d.getSeconds()).slice(-2)+"+08:00"
        )
    }

    this.convertToSGT = function (dateString){
        d = new Date(dateString)
        return(
            d.getFullYear()
            +"-"+
            ("0" + (d.getMonth() + 1)).slice(-2)
            +"-"+
            ("0" + d.getDate()).slice(-2)
            +"T"+
            ("0" + d.getHours()).slice(-2)
            +":"+
            ("0" + d.getMinutes()).slice(-2)
            +":"+
            ("0" + d.getSeconds()).slice(-2)+"+08:00"
        )
    }
}

module.exports = (
    dateutil
)
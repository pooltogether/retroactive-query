const file = require("./results")

console.log("starting parse")

const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

let balances = {} // this object holds a mapping from address to {value, timestamp, transferType(mint, burn, transfer-out, transfer-in)}

let mint_count =0
let burn_count =0
let transfer_count =0 

const CUTOFF_BLOCK = 15000000

// extract transfer  
file.forEach((transfer,index) => {
    console.log("parsing transfer: ", index)
    // if(transfer.block_number < CUTOFF_BLOCK){
        
        // if transfer is MINT
        if(transfer.from_address === ZERO_ADDRESS){
            mint_count++
            const key = transfer.to_address
            if(!balances[key]){
                balances[key] = [] 
            }
            balances[key].push({value: transfer.value, timestamp: transfer.block_number, type: "mint"})
        }
        // if transfer is BURN
        else if(transfer.to_address === ZERO_ADDRESS){
            const key = transfer.from_address
            burn_count++
            
            if(!balances[key]){
                balances[key] = [] 
            }
            balances[key].push({value: transfer.value, timestamp: transfer.block_number, type: "burn"})
        }

        else{ // its a ticket transfer
            // reduce the balance of sender 
            const key = transfer.from_address
            transfer_count++
            if(!balances[key]){
                balances[key] = [] 
            }
            balances[key].push({value: transfer.value, timestamp: transfer.block_number, type: "transfer-out"})

            // increase the balance of receiver 
            const receiver = transfer.to_address

            if(!balances[receiver]){
                balances[receiver] = [] 
            }
            balances[receiver].push({value: transfer.value, timestamp: transfer.block_number, type: "transfer-in"})
        }    
    // }

});


// now sum balances
let summedBalances = {} // this object holds a mapping of address : balance at the cutoff block
Object.keys(balances).forEach(address => {
    //console.log(address, balances[address]);
    balances[address].forEach((update)=>{
        if(!summedBalances[address]){
            summedBalances[address] = { 
                                        balance:0,
                                        duration:0
                                    }
        }
        
        if(update.type === "mint"){
            summedBalances[address].balance += parseInt(update.value)
            // summedBalances[address].duration += parseInt(update.timestamp)
        }
        if(update.type === "burn"){
            summedBalances[address].balance -= parseInt(update.value)
            // summedBalances[address].duration -= parseInt(update.timestamp)
        }
        if(update.type === "transfer-out"){
            summedBalances[address].balance -= parseInt(update.value)
            // summedBalances[address].duration -= parseInt(update.timestamp)
        }
        if(update.type === "transfer-in"){
            summedBalances[address].balance += parseInt(update.value)
            // summedBalances[address].duration += parseInt(update.timestamp)
        }       
    })

});

// simulate burn event
let finalBalances = balances // create copy of balances to insert burn event
let simmed_burns = 0
Object.keys(summedBalances).forEach(address => {
    if(summedBalances[address].balance != 0){ // if they have balance remaining
        simmed_burns++
        finalBalances[address].push({value: summedBalances[address].balance, timestamp: CUTOFF_BLOCK, type: "burn"})
    }
})



// now find token block times per address
let result = {} // result hold the address: amount blocks 
/*
   if(from_address == ZERO){ x0 = value * timestamp, balChange = value }
   if(to_address == ZERO){ x1 = value * timestamp , balChange = -value}   
   if(to_address == other_address) {x2 = value * timestamp, balChange = -value } 
*/


Object.keys(finalBalances).forEach(address => {
    // console.log(address, finalBalances[address]);
    finalBalances[address].forEach((update)=>{
        if(!result[address]){
            result[address] = {timebalance:0}
        }

        if(update.type === "mint"){
            result[address].timebalance += parseInt(update.value) * parseInt(update.timestamp)
        }
        if(update.type === "burn"){
            result[address].timebalance -= parseInt(update.value)  * parseInt(update.timestamp)
        }
        if(update.type === "transfer-out"){
            result[address].timebalance -= parseInt(update.value)  * parseInt(update.timestamp)         
        }
        if(update.type === "transfer-in"){
            result[address].timebalance += parseInt(update.value)  * parseInt(update.timestamp)
        }   
        
    })

});

// now get absolute of these 
Object.keys(result).forEach(address => {
    result[address].timebalance =  Math.abs(result[address].timebalance)
})





//summary
//console.log(summedBalances)
console.log("simmed burns ", simmed_burns)
console.log("total transfers ", file.length)
console.log("burn count: ", burn_count)
console.log("mint count: ", mint_count)
console.log("transfer count: ", transfer_count)

console.log("number of balances: ", Object.keys(summedBalances).length)

// find number of balances equal to zero
let count_zero_balance = 0
Object.keys(summedBalances).forEach(address => {
    if(summedBalances[address].balance === 0){
        count_zero_balance++
    }
})
console.log("zero balance count ", count_zero_balance)


console.log(result)
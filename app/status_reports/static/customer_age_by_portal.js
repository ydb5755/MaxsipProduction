async function getTimeLastUpdatedStatusReports(){
    const result = await fetch('/status_reports/get_time_last_updated_status_reports');
    const data = await result.json();
  
    var targets = document.getElementsByClassName('lastUpdated');
    for (let i = 0; i < targets.length; i++){
      targets[i].innerText = `Last updated at ${data['last_updated']}`
    }
  }
  
function getCurrentDate(){
    var date = new Date().toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric'
    });


    var target = document.getElementById('current-date-target');
    target.innerText = date;

}
async function getTotalActiveCustomerCount(){
    const result = await fetch('/status_reports/get_total_active_customer_count');
    const data = await result.json();
    var activeCC = document.getElementById('totalActiveCustomerCount');
    activeCC.innerText = data.total.toLocaleString();
    activeCC.classList.remove('loading');
    var firstSibling = document.getElementById('totalActiveCustomerCount1');
    firstSibling.innerText = data['0-30'].toLocaleString();
    var secondSibling = document.getElementById('totalActiveCustomerCount2');
    secondSibling.innerText = data['31-90'].toLocaleString();
    var thirdSibling = document.getElementById('totalActiveCustomerCount3');
    thirdSibling.innerText = data['91-180'].toLocaleString();
    var fourthSibling = document.getElementById('totalActiveCustomerCount4');
    fourthSibling.innerText = data['181-360'].toLocaleString();
    var fifthSibling = document.getElementById('totalActiveCustomerCount5');
    fifthSibling.innerText = data['360+'].toLocaleString();
}
  
async function getTelgooActiveCounts(){
    const result = await fetch('/status_reports/get_telgoo_active_customer_count');
    const data = await result.json();
  
    var activeCC = document.getElementById('telgooActive1');
    activeCC.classList.remove('loading')
    activeCC.innerText = data.total.toLocaleString();
    var firstSibling = document.getElementById('telgooActive2');
    firstSibling.innerText = data['0-30'].toLocaleString();
    var secondSibling = document.getElementById('telgooActive3');
    secondSibling.innerText = data['31-90'].toLocaleString();
    var thirdSibling = document.getElementById('telgooActive4');
    thirdSibling.innerText = data['91-180'].toLocaleString();
    var fourthSibling = document.getElementById('telgooActive5');
    fourthSibling.innerText = data['181-360'].toLocaleString();
    var fifthSibling = document.getElementById('telgooActive6');
    fifthSibling.innerText = data['360+'].toLocaleString();
}
  
async function getTerracomActiveCounts(){
    const result = await fetch('/status_reports/get_terracom_active_customer_count');
    const data = await result.json();
  
    var activeCC = document.getElementById('terracomActive1');
    activeCC.classList.remove('loading')
    activeCC.innerText = data.total.toLocaleString();
    var firstSibling = document.getElementById('terracomActive2');
    firstSibling.innerText = data['0-30'].toLocaleString();
    var secondSibling = document.getElementById('terracomActive3');
    secondSibling.innerText = data['31-90'].toLocaleString();
    var thirdSibling = document.getElementById('terracomActive4');
    thirdSibling.innerText = data['91-180'].toLocaleString();
    var fourthSibling = document.getElementById('terracomActive5');
    fourthSibling.innerText = data['181-360'].toLocaleString();
    var fifthSibling = document.getElementById('terracomActive6');
    fifthSibling.innerText = data['360+'].toLocaleString();
}
  
async function getUnavoActiveCounts(){
    const result = await fetch('/status_reports/get_unavo_active_customer_count');
    const data = await result.json();
  
    var activeCC = document.getElementById('unavoActive1');
    activeCC.classList.remove('loading')
    activeCC.innerText = data.total.toLocaleString();
    var firstSibling = document.getElementById('unavoActive2');
    firstSibling.innerText = data['0-30'].toLocaleString();
    var secondSibling = document.getElementById('unavoActive3');
    secondSibling.innerText = data['31-90'].toLocaleString();
    var thirdSibling = document.getElementById('unavoActive4');
    thirdSibling.innerText = data['91-180'].toLocaleString();
    var fourthSibling = document.getElementById('unavoActive5');
    fourthSibling.innerText = data['181-360'].toLocaleString();
    var fifthSibling = document.getElementById('unavoActive6');
    fifthSibling.innerText = data['360+'].toLocaleString();
}
  
document.addEventListener('DOMContentLoaded', async function() {
    getCurrentDate();
    getTimeLastUpdatedStatusReports();
    getTotalActiveCustomerCount();
    getTelgooActiveCounts();
    getTerracomActiveCounts();
    getUnavoActiveCounts();
  });
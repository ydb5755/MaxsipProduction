
function secondsInputListeners(){
  const secondsInput = document.getElementById('seconds-input');
  secondsInput.value = "10";
  secondsInput.addEventListener('input', function(e) {
    console.log(e.inputType)
    var stringArray = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0"];
    if (!stringArray.includes(e.data) && e.inputType != "deleteContentBackward" && e.inputType != "deleteContentForward") {
      secondsInput.value = secondsInput.value.slice(0,-1);
      console.log(secondsInput.value);
    }
  })
  secondsInput.addEventListener('change', function(e){
    if (Number(e.target.value) < 10) {
      secondsInput.value = "10";
    }
  })
}
function startButtonListener(){
  const startBtn = document.getElementById('start-button');
  startBtn.addEventListener('click', function() {
    
  })
}

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

async function getActiveCustomerCount(){
    var activeCC = document.getElementById('activeCustomerCount');
    const result = await fetch('/status_reports/get_active_customer_count');
    const data = await result.json();
    activeCC.innerText = data.current.toLocaleString();
    activeCC.classList.remove('loading');
  
    var firstSibling = document.getElementById('activeCustomerCount1');
    var firstParent = firstSibling.parentElement;
    var firstIcon = document.createElement('i');
    let percentYesterday = (data.current - data.yesterday)/data.current * 100;
    firstSibling.innerText = `${percentYesterday.toFixed(2)}%\n${(data.current - data.yesterday).toLocaleString()}`;
    if ((data.current - data.yesterday) < 0) {
      firstSibling.style.color = 'red'
      firstIcon.className = "fa-solid fa-arrow-down fa-2xl";
      firstIcon.style.color = 'red';
      firstParent.appendChild(firstIcon);
    } else if ((data.current - data.yesterday) > 0){
      firstIcon.className = "fa-solid fa-arrow-up fa-2xl";
      firstIcon.style.color = 'blue';
      firstParent.appendChild(firstIcon);
    }
  
    var secondSibling = document.getElementById('activeCustomerCount2');
    var secondParent = secondSibling.parentElement;
    var secondIcon = document.createElement('i');
    let percentLastWeek = (data.current - data.last_week)/data.current * 100;
    secondSibling.innerText = `${percentLastWeek.toFixed(2)}%\n${(data.current - data.last_week).toLocaleString()}`;
    if ((data.current - data.last_week) < 0) {
      secondSibling.style.color = 'red'
      secondIcon.className = "fa-solid fa-arrow-down fa-2xl";
      secondIcon.style.color = 'red';
      secondParent.appendChild(secondIcon);
    } else if ((data.current - data.last_week) > 0){
      secondIcon.className = "fa-solid fa-arrow-up fa-2xl";
      secondIcon.style.color = 'blue';
      secondParent.appendChild(secondIcon);
    }
  
    var thirdSibling = document.getElementById('activeCustomerCount3');
    var thirdParent = thirdSibling.parentElement;
    var thirdIcon = document.createElement('i');
    let percentlastMonth = (data.current - data.last_month)/data.current * 100;
    thirdSibling.innerText = `${percentlastMonth.toFixed(2)}%\n${(data.current - data.last_month).toLocaleString()}`;
    if ((data.current - data.last_month) < 0) {
      thirdSibling.style.color = 'red'
      thirdIcon.className = "fa-solid fa-arrow-down fa-2xl";
      thirdIcon.style.color = 'red';
      thirdParent.appendChild(thirdIcon);
    } else if ((data.current - data.last_month) > 0){
      thirdIcon.className = "fa-solid fa-arrow-up fa-2xl";
      thirdIcon.style.color = 'blue';
      thirdParent.appendChild(thirdIcon);
    }
}
  
async function getTotalCustomerCount(){
  var totalCC = document.getElementById('totalCustomerCount');
  const result = await fetch('/status_reports/get_total_customer_count');
  const data = await result.json();
  totalCC.innerText = data.current.toLocaleString();
  totalCC.classList.remove('loading');

  var firstSibling = document.getElementById('totalCustomerCount1');
  var firstParent = firstSibling.parentElement;
  var firstIcon = document.createElement('i');
  let percentYesterday = (data.current - data.yesterday)/data.current * 100;
  firstSibling.innerText = `${percentYesterday.toFixed(2)}%\n${(data.current - data.yesterday).toLocaleString()}`;
  if ((data.current - data.yesterday) < 0) {
    firstSibling.style.color = 'red'
    firstIcon.className = "fa-solid fa-arrow-down fa-2xl";
    firstIcon.style.color = 'red';
    firstParent.appendChild(firstIcon);
  } else if ((data.current - data.yesterday) > 0){
    firstIcon.className = "fa-solid fa-arrow-up fa-2xl";
    firstIcon.style.color = 'blue';
    firstParent.appendChild(firstIcon);
  }

  var secondSibling = document.getElementById('totalCustomerCount2');
  var secondParent = secondSibling.parentElement;
  var secondIcon = document.createElement('i');
  let percentLastWeek = (data.current - data.last_week)/data.current * 100;
  secondSibling.innerText = `${percentLastWeek.toFixed(2)}%\n${(data.current - data.last_week).toLocaleString()}`;
  if ((data.current - data.last_week) < 0) {
    secondSibling.style.color = 'red'
    secondIcon.className = "fa-solid fa-arrow-down fa-2xl";
    secondIcon.style.color = 'red';
    secondParent.appendChild(secondIcon);
  } else if ((data.current - data.last_week) > 0){
    secondIcon.className = "fa-solid fa-arrow-up fa-2xl";
    secondIcon.style.color = 'blue';
    secondParent.appendChild(secondIcon);
  }

  var thirdSibling = document.getElementById('totalCustomerCount3');
  var thirdParent = thirdSibling.parentElement;
  var thirdIcon = document.createElement('i');
  let percentlastMonth = (data.current - data.last_month)/data.current * 100;
  thirdSibling.innerText = `${percentlastMonth.toFixed(2)}%\n${(data.current - data.last_month).toLocaleString()}`;
  if ((data.current - data.last_month) < 0) {
    thirdSibling.style.color = 'red'
    thirdIcon.className = "fa-solid fa-arrow-down fa-2xl";
    thirdIcon.style.color = 'red';
    thirdParent.appendChild(thirdIcon);
  } else if ((data.current - data.last_month) > 0){
    thirdIcon.className = "fa-solid fa-arrow-up fa-2xl";
    thirdIcon.style.color = 'blue';
    thirdParent.appendChild(thirdIcon);
  }
}
  
async function getThirtyDayCustomerCount(){
  var thirtyDayCC = document.getElementById('thirtyDayCustomerCount');
  const result = await fetch('/status_reports/get_thirty_day_customer_count');
  const data = await result.json();
  thirtyDayCC.innerText = data.current.toLocaleString();
  thirtyDayCC.classList.remove('loading');

  var firstSibling = document.getElementById('thirtyDayCustomerCount1');
  var firstParent = firstSibling.parentElement;
  var firstIcon = document.createElement('i');
  let percentYesterday = (data.current - data.yesterday)/data.current * 100;
  firstSibling.innerText = `${percentYesterday.toFixed(2)}%\n${(data.current - data.yesterday).toLocaleString()}`;
  if ((data.current - data.yesterday) < 0) {
    firstSibling.style.color = 'red'
    firstIcon.className = "fa-solid fa-arrow-down fa-2xl";
    firstIcon.style.color = 'red';
    firstParent.appendChild(firstIcon);
  } else if ((data.current - data.yesterday) > 0){
    firstIcon.className = "fa-solid fa-arrow-up fa-2xl";
    firstIcon.style.color = 'blue';
    firstParent.appendChild(firstIcon);
  }

  var secondSibling = document.getElementById('thirtyDayCustomerCount2');
  var secondParent = secondSibling.parentElement;
  var secondIcon = document.createElement('i');
  let percentLastWeek = (data.current - data.last_week)/data.current * 100;
  secondSibling.innerText = `${percentLastWeek.toFixed(2)}%\n${(data.current - data.last_week).toLocaleString()}`;
  if ((data.current - data.last_week) < 0) {
    secondSibling.style.color = 'red'
    secondIcon.className = "fa-solid fa-arrow-down fa-2xl";
    secondIcon.style.color = 'red';
    secondParent.appendChild(secondIcon);
  } else if ((data.current - data.last_week) > 0){
    secondIcon.className = "fa-solid fa-arrow-up fa-2xl";
    secondIcon.style.color = 'blue';
    secondParent.appendChild(secondIcon);
  }

  var thirdSibling = document.getElementById('thirtyDayCustomerCount3');
  var thirdParent = thirdSibling.parentElement;
  var thirdIcon = document.createElement('i');
  let percentlastMonth = (data.current - data.last_month)/data.current * 100;
  thirdSibling.innerText = `${percentlastMonth.toFixed(2)}%\n${(data.current - data.last_month).toLocaleString()}`;
  if ((data.current - data.last_month) < 0) {
    thirdSibling.style.color = 'red'
    thirdIcon.className = "fa-solid fa-arrow-down fa-2xl";
    thirdIcon.style.color = 'red';
    thirdParent.appendChild(thirdIcon);
  } else if ((data.current - data.last_month) > 0){
    thirdIcon.className = "fa-solid fa-arrow-up fa-2xl";
    thirdIcon.style.color = 'blue';
    thirdParent.appendChild(thirdIcon);
  }
}

async function makeSpace(){
  const spacerElement = document.getElementById('spacer');
  const sourceElem = document.getElementById('auto-run-shell');
  spacerElement.style.width = sourceElem.offsetWidth + "px";
}

document.addEventListener('DOMContentLoaded', async function() {
    getCurrentDate();
    getTimeLastUpdatedStatusReports();
    getActiveCustomerCount();
    getTotalCustomerCount();
    getThirtyDayCustomerCount();
    makeSpace();
    secondsInputListeners();
});
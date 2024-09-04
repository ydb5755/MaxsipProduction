
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

async function getEveryProgramData(){
  const tableTarget = document.getElementById('table-body')
  const result = await fetch('/status_reports/get_program_data');
  const data = await result.json();
  for (const program in data) {
    var row = document.createElement('tr');
    var rowHeader = document.createElement('th');
    rowHeader.classList = 'header-column text-center';
    var programHeader = document.createElement('h3');
    programHeader.innerText = `${program} Active Customers`;
    rowHeader.appendChild(programHeader);
    var currentValue = document.createElement('h4');
    currentValue.innerText = data[program][0].toLocaleString();
    rowHeader.appendChild(currentValue);
    row.appendChild(rowHeader);
    
    for (let i = 1; i < data[program].length; i++){
      var pieceOfData = document.createElement('td');
      pieceOfData.classList = 'text-center';
      var numArrowContainer = document.createElement('div');
      numArrowContainer.classList = 'container-for-nums-and-arrow';
      var numDiv = document.createElement('div');
      numDiv.classList = 'child';
      var percentage = (data[program][0] - data[program][i])/data[program][0]*100;
      numDiv.innerText = `${percentage.toFixed(2)}%\n${(data[program][0] - data[program][i]).toLocaleString()}`;
      numArrowContainer.appendChild(numDiv);

      var iconElem = document.createElement('i');
      if ((data[program][0] - data[program][i]) < 0) {
        numDiv.style.color = 'red';
        iconElem.className = "fa-solid fa-arrow-down fa-2xl";
        iconElem.style.color = 'red';
        numArrowContainer.appendChild(iconElem);
      } else if ((data[program][0] - data[program][i]) > 0){
        iconElem.className = "fa-solid fa-arrow-up fa-2xl";
        iconElem.style.color = 'blue';
        numArrowContainer.appendChild(iconElem);
      }

      pieceOfData.appendChild(numArrowContainer);
      row.appendChild(pieceOfData);
    }

    tableTarget.appendChild(row);
  }
}


document.addEventListener('DOMContentLoaded', async function() {
    getCurrentDate();
    getTimeLastUpdatedStatusReports();
    getEveryProgramData()
  });
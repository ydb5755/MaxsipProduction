
var state = {
  'requested-date':getDate(0),
  'days-in-past':0,
  'date-lower-limit':getDateLowerLimit()
}

const rightArrow = document.getElementById('date-arrow-right');
const leftArrow = document.getElementById('date-arrow-left');
const showMoreButton = document.getElementById('show-more');
const agentRadio = document.getElementById('agent-radio');
const masterAgentRadio = document.getElementById('master-agent-radio');
const tableBody = document.getElementById('table-body');
const topLeftTitle = document.getElementById('top-left-title');
const totalTarget = document.getElementById('total-active-target')
const totalAgentTarget = document.getElementById('total-agent-target')

async function getDateLowerLimit(){
  const result = await fetch('/agent_reports/get_date_lower_limit');
  const data = await result.json();
  state['date-lower-limit'] = data.result
}

function getDate(days_in_past){
  var today = new Date();
  let requested_day = new Date(today.setDate(today.getDate()-days_in_past));
  var date = requested_day.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });
  return date;
  }

function setDate(days_in_past){
  state['requested-date'] = getDate(days_in_past);
  var target = document.getElementById('current-date-target');
  target.innerText = state['requested-date'];
}
async function getTotalActive(agent_or_master_agent, day_request_offset){
  const result = await fetch(`/agent_reports/get_total_active_customer_count_for_agent_and_master_agent/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  totalTarget.innerText = data.count.toLocaleString();
}
async function getTotalAgents(agent_or_master_agent, day_request_offset){
  const result = await fetch(`/agent_reports/get_total_number_of_agents_for_agent_customer_analysis/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  totalAgentTarget.innerText = data.count.toLocaleString();
}
async function getFiveAgents(offset, agent_or_master_agent, day_request_offset){
  var table = document.getElementById('table-body');
  rightArrow.classList = '';
  leftArrow.classList = '';
  const result = await fetch(`/agent_reports/get_five_agents_for_active/${offset}/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  var agents = data.agents
  for (let i = 0; i < agents.length; i++) {
    var row = document.createElement('tr');
    var currentValue = agents[i][1];
    for (let j = 0; j < agents[i].length; j++){
      var td = document.createElement('td');
      td.classList = 'text-center';
      var container = document.createElement('div');
      container.classList = 'nums-and-arrows';
      var numberContainer = document.createElement('div');
      numberContainer.classList = 'child';
      container.appendChild(numberContainer);
      var icon = document.createElement('i');
      if(j>1 && j < 5){
        var percent = ((currentValue - agents[i][j]) / currentValue * 100);
        numberContainer.innerText = `${percent.toFixed(2)}%\n${(currentValue - agents[i][j]).toLocaleString()}`;
        if ((currentValue - agents[i][j]) < 0) {
          numberContainer.style.color = 'red';
          icon.className = "fa-solid fa-arrow-down fa-2xl";
          icon.style.color = 'red';
          container.appendChild(icon);
        } else if ((currentValue - agents[i][j]) > 0){
          icon.className = "fa-solid fa-arrow-up fa-2xl";
          icon.style.color = 'blue';
          container.appendChild(icon);
        }
      } else if(j > 5){
        numberContainer.innerText = agents[i][j].toLocaleString()
      } else {
        numberContainer.innerText = agents[i][j].toLocaleString();
      }
      row.appendChild(td);
      td.appendChild(container);
    }
    table.appendChild(row);
  }
  if (state['days-in-past'] != 0){
    rightArrow.classList = 'fa-solid fa-arrow-right fa-xl';
  }
  if (state['days-in-past'] != state['date-lower-limit']){
    leftArrow.className = 'fa-solid fa-arrow-left fa-xl';
  }
  getTotalActive(agent_or_master_agent, day_request_offset);
  getTotalAgents(agent_or_master_agent, day_request_offset)
}

document.addEventListener('DOMContentLoaded', async () => {
  setDate(0);
  getFiveAgents(0, 'agent', 0);
});

rightArrow.addEventListener('click', () => {
  state['days-in-past']--;
  state['requested-date'] = getDate(state['days-in-past']);
  setDate(state['days-in-past']);
  tableBody.innerHTML = '';
  if (agentRadio.checked){
    getFiveAgents(0, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(0, 'master_agent', state['days-in-past']);
  }

});

leftArrow.addEventListener('click', () => {
  state['days-in-past']++;
  state['requested-date'] = getDate(state['days-in-past']);
  setDate(state['days-in-past']);
  tableBody.innerHTML = '';
  if (agentRadio.checked){
    getFiveAgents(0, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(0, 'master_agent', state['days-in-past']);
  }
})

showMoreButton.addEventListener('click', () => {
  const offset = tableBody.childElementCount;
  if (agentRadio.checked){
    getFiveAgents(offset, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(offset, 'master_agent', state['days-in-past']);
  }
});

agentRadio.addEventListener('change', () => {
  tableBody.innerHTML = '';
  getFiveAgents(0, 'agent', state['days-in-past']);
  topLeftTitle.innerText = 'Agent Information'
});

masterAgentRadio.addEventListener('change', () => {
  tableBody.innerHTML = '';
  getFiveAgents(0, 'master_agent', state['days-in-past']);
  topLeftTitle.innerText = 'Master Agent Information'
});

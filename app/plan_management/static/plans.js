var state = {
    'offset':0,
    'view':'both',
    'totalPlansVisible':0,
    'assignDisabled':true
}
const quantityTarget = document.getElementById('plan-qty');
const assignedQuantityTarget = document.getElementById('view-assigned-quantity');
const unAssignedQuantityTarget = document.getElementById('view-unassigned-quantity');
const tableTarget = document.getElementById('table-body');
const sourceDB = document.getElementById('source-database');
const assignButton = document.getElementById('assign-button'); 
const assignedCheckbox = document.getElementById('assigned-checkbox'); 
const unAssignedCheckbox = document.getElementById('unassigned-checkbox'); 
const nextButton = document.getElementById('next-button');
const previousButton = document.getElementById('previous-button');

function setViewAssignCheckBoxes(){
    if (assignedCheckbox.checked === true && unAssignedCheckbox.checked === true){
        state['view'] = 'both';
    } else if(assignedCheckbox.checked === true){
        state['view'] = 'assigned';
    } else if(unAssignedCheckbox.checked === true){
        state['view'] = 'unassigned';
    } else{
        state['view'] = 'none'
    }
}

async function getProgramNames(){
    var result = await fetch('/plan_management/get_program_names');
    var data = await result.json();
    return data.data
}

function setTotalPlansVisible(){
    var total = 0;
    if (assignedCheckbox.checked === true){
        total += parseInt(assignedQuantityTarget.innerText);
    }
    if (unAssignedCheckbox.checked === true){
        total += parseInt(unAssignedQuantityTarget.innerText);
    }
    state['totalPlansVisible'] = total;
}

async function getPlanQuantity(){
    var result = await fetch(`/plan_management/get_plan_quantities/${sourceDB.innerText}`);
    var data = await result.json();
    const totalPlansVisible = data.unassigned_quantity + data.assigned_quantity;
    quantityTarget.innerText = totalPlansVisible;
    assignedQuantityTarget.innerText = data.assigned_quantity;
    unAssignedQuantityTarget.innerText = data.unassigned_quantity;
}

async function getFivePlans(offset){
    const programNameMap = await getProgramNames();
    var result = await fetch(`/plan_management/get_five_plans/${sourceDB.innerText}/${state['view']}/${offset}`);
    var data = await result.json();
    const plans = data.plans;
    for (let i = 0; i < plans.length; i++){
        var row = document.createElement('tr');
        var idCell = document.createElement('td');
        var nameCell = document.createElement('td');
        var customerActiveCountCell = document.createElement('td');
        var customerInactiveCountCell = document.createElement('td');
        idCell.innerText = plans[i][0];
        nameCell.innerText = plans[i][1];
        customerActiveCountCell.innerText = plans[i][2].toLocaleString();
        customerInactiveCountCell.innerText = plans[i][3].toLocaleString();
        row.appendChild(idCell);
        row.appendChild(nameCell);
        row.appendChild(customerActiveCountCell);
        row.appendChild(customerInactiveCountCell);
        for (let j = 0; j < Object.keys(programNameMap).length; j++){
            var radioInputCell = document.createElement('td');
            radioInputCell.style.textAlign = 'center';
            radioInputCell.className = 'color-transform';
            if (state['assignDisabled'] == false){
                radioInputCell.style.backgroundColor = 'rgb(144, 238, 144)';
            }
            var radioInput = document.createElement('input');
            radioInput.type = 'radio';
            radioInput.disabled = state['assignDisabled'];
            radioInput.className = 'radio-input';
            radioInput.name = plans[i][1];
            radioInput.addEventListener('click', async () => {
                saveSinglePlan(plans[i][0], programNameMap[j]);
            });
            if (plans[i][4] === programNameMap[j]){
                radioInput.checked = true;
            }
            radioInputCell.appendChild(radioInput)
            row.appendChild(radioInputCell);
        }
        tableTarget.appendChild(row);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    await getPlanQuantity();
    await getFivePlans(0);
    setTotalPlansVisible();
});

async function onChangeCheckbox(){
    setViewAssignCheckBoxes();
    setTotalPlansVisible();
    state['offset'] = 0;
    tableTarget.innerHTML = '';
    getFivePlans(0);
    previousButton.disabled = true;
    if (state['totalPlansVisible'] <= 5){
        nextButton.disabled = true;
    } else {
        nextButton.disabled= false;
    }
}
assignedCheckbox.addEventListener('change', onChangeCheckbox);
unAssignedCheckbox.addEventListener('change', onChangeCheckbox);

assignButton.addEventListener('click', () => {
    var checkboxes = document.getElementsByClassName('radio-input');
    for (let i = 0; i < checkboxes.length; i++){
        checkboxes[i].disabled = false;
    }
    var checkboxes = document.getElementsByClassName('color-transform');
    for (let i = 0; i < checkboxes.length; i++){
        checkboxes[i].style.backgroundColor = 'rgb(144, 238, 144)';
    }
    state['assignDisabled'] = false;
});

nextButton.addEventListener('click', () => {
    if (previousButton.disabled === true){
        previousButton.disabled = false;
    }
    tableTarget.innerHTML = '';
    state['offset'] += 5;
    if (state['offset'] + 5 >= state['totalPlansVisible']) {
        nextButton.disabled = true;
    }
    getFivePlans(state['offset']);
});

previousButton.addEventListener('click', () => {
    tableTarget.innerHTML = '';
    state['offset'] -= 5;
    if (state['offset'] === 0){
        previousButton.disabled = true;
    }
    if (nextButton.disabled === true) {
        nextButton.disabled = false;
    }
    getFivePlans(state['offset']);
});

async function saveSinglePlan(id, program){

    await fetch('/plan_management/update_plans',
        {
            method: 'POST',
            headers: {
                'accept': 'application/json',
                'content-type': 'application/json'
            },
            body:JSON.stringify({
                'id':id,
                'program':program
            })
        }
    ).then(() => {
        getPlanQuantity();
    })
}
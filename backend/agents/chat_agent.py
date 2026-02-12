import os
import json
from datetime import datetime
from openai import AzureOpenAI
from config import get_azure_client, get_deployment_name
from table_db import (
    get_all_tickets_df, 
    update_multiple_fields, 
    get_kpi_metrics, 
    get_team_list,
    search_invoices,
    intelligent_assign_tickets
)

class ChatAIAgent:
    def __init__(self, user_info: dict):
        self.client = get_azure_client()
        self.deployment = get_deployment_name()
        self.user_info = user_info
        self.role = user_info.get("role", "employee").lower()
        self.name = user_info.get("name", "Unknown")
        self.team = user_info.get("team", "Unknown")

        # Handle admin/manager team visibility
        self.team_str = ", ".join(self.team) if isinstance(self.team, list) else self.team

        # Get all valid teams from data to help the agent
        try:
            self.all_teams = get_team_list()
        except:
            self.all_teams = []

        self.system_prompt = f"""
        You are an Query Management Chat Assistant.
        Current User: {self.name}
        Role: {self.role}
        Team(s): {self.team_str}
        Available Teams in System: {self.all_teams}

        Your capabilities based on role:
        
        IF ROLE IS employee:
        - You can list tickets assigned to the user.
        - You can update status, priority, or category of their own tickets.
        - You can help them close tickets.
        - You can reorder or filter their tickets as requested.
        - You can check invoice/PO status using 'search_invoices' for their own queries.
        - DO NOT allow them to see or modify tickets belonging to other people or teams.

        IF ROLE IS manager:
        - You have access to tickets in their team: {self.team_str}.
        - You can reassign tickets to employees WITHIN their team.
        - You can change ticket status, priority, and assigned person for their team's tickets.
        - You can provide performance metrics and KPIs for their team.

        IF ROLE IS admin:
        - You have UNRESTRICTED access to ALL tickets across ALL teams in the system.
        - You can reassign tickets between ANY teams (e.g., from AP to AR or IT).
        - You can change any property of any ticket.
        - You can provide analytics for specific teams or the entire organization.
        - You have full access to search all invoices.

        CRITICAL POLICY:
        - If a user asks to change a ticket (e.g. "close it", "set priority to high", "reassign text"), you **MUST** call the 'update_ticket_properties' tool.
        - After calling the tool, confirm to the user that the spreadsheet has been updated.
        - Always ensure the correct Ticket ID is used for updates.

        RESPONSE FORMATTING GUIDELINES:
        - **Use Markdown**: Always use Markdown features for clarity. 
            * Use bold text for Ticket IDs (e.g., **TCK-1001**) and Statuses.
            * Use bulleted lists for multiple tickets or items.
            * Use tables if comparing data or showing more than 3 tickets.
        - **Information Density**: Decided based on the user's specific query.
            * If they ask "Who is in my team?", just provide names.
            * If they ask "Give me details on my open tickets", show ID, Priority, and Brief Description.
            * Do NOT output a wall of text. Structure your info with clear headers and spacing.
        
        If a request is outside the user's role permissions, politely explain why you cannot perform it.
        Always provide professional responses in EY style.
        """

    def get_tool_definitions(self):
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "list_tickets",
                    "description": "Retrieve tickets with optional filters.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "assigned_to": {"type": "string", "description": "Filter by person name"},
                            "team": {"type": "string", "description": "Filter by team name"},
                            "status": {"type": "string", "description": "Filter by status (Open/Closed)"}
                        }
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "update_ticket_properties",
                    "description": "Update specific fields of a ticket in the spreadsheet.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "ticket_id": {"type": "string"},
                            "updates": {
                                "type": "object",
                                "properties": {
                                    "Ticket Status": {"type": "string", "enum": ["Open", "Closed"]},
                                    "Priority": {"type": "string", "enum": ["High", "Medium", "Low"]},
                                    "User Name": {"type": "string", "description": "Assign to person"},
                                    "Assigned Team": {"type": "string", "description": "Assign to team"},
                                    "Category": {"type": "string", "description": "Ticket category"}
                                }
                            }
                        },
                        "required": ["ticket_id", "updates"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "search_invoices",
                    "description": "Search the invoice/PO database for specific details.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "Invoice Number": {"type": "string"},
                            "Customer Name": {"type": "string"},
                            "Vendor Name": {"type": "string"},
                            "Payment Status": {"type": "string"},
                            "PO Number": {"type": "string"}
                        }
                    }
                }
            }
        ]

        if self.role == "manager" or self.role == "admin":
            tools.append({
                "type": "function",
                "function": {
                    "name": "get_analytics_report",
                    "description": "Get KPI metrics and performance data for the team.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "team_name": {"type": "string", "description": "Optional team name to filter metrics"}
                        }
                    }
                }
            })
            tools.append({
                "type": "function",
                "function": {
                    "name": "get_available_resources",
                    "description": "List available teams or employees for reassinement.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "team_name": {"type": "string", "description": "Optional team to see members of"}
                        }
                    }
                }
            })
            tools.append({
                "type": "function",
                "function": {
                    "name": "intelligent_assign_tickets",
                    "description": "Automatically assign unassigned open tickets to employees to balance workload.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "team_name": {"type": "string", "description": "Optional team name to balance workload for"}
                        }
                    }
                }
            })
        
        return tools

    def run_chat(self, user_message: str, history: list = None):
        if history is None:
            history = []
        
        messages = [{"role": "system", "content": self.system_prompt}]
        messages.extend(history)
        messages.append({"role": "user", "content": user_message})

        total_tokens = 0

        for turn in range(5):
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                tools=self.get_tool_definitions(),
                tool_choice="auto"
            )
            
            # Track tokens
            if response.usage:
                total_tokens += response.usage.total_tokens

            msg = response.choices[0].message
            
            # Convert OpenAI objects to plain dicts for JSON serialization (history)
            msg_dict = msg.model_dump()
            # Remove None values to keep history clean if needed, 
            # but model_dump(exclude_none=True) is safer
            messages.append(msg_dict)

            if not msg.tool_calls:
                # Return the content, history, and total tokens
                return msg.content, [m if isinstance(m, dict) else m.model_dump() for m in messages[1:]], total_tokens

            for tool_call in msg.tool_calls:
                func_name = tool_call.function.name
                try:
                    args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError as je:
                    print(f"ERROR: AI generated invalid JSON for tools: {tool_call.function.arguments}")
                    return "I apologize, but I encountered a technical error while processing that request. Please try rephrasing.", messages[1:]
                
                result = ""
                if func_name == "list_tickets":
                    df = get_all_tickets_df()
                    print(f"DEBUG: ChatAgent list_tickets called with filters: {args}")
                    
                    # Apply role-based mandatory filters
                    if self.role == "employee":
                        df = df[df["User Name"].str.lower() == self.name.lower()]
                    elif self.role == "manager":
                        # Managers are strictly limited to their own team(s)
                        if isinstance(self.team, list):
                            df = df[df["Assigned Team"].str.lower().isin([t.lower() for t in self.team])]
                        else:
                            df = df[df["Assigned Team"].str.lower().str.contains(self.team.lower(), na=False)]
                    
                    # Admins have no mandatory filter (can see everything)

                    # Apply optional filters from AI (within the already filtered range)
                    if "assigned_to" in args:
                        df = df[df["User Name"].str.lower() == str(args["assigned_to"]).lower()]
                    if "team" in args:
                        # Only filter by team if it doesn't violate role restriction
                        df = df[df["Assigned Team"].str.lower().str.contains(str(args["team"]).lower(), na=False)]
                    if "status" in args:
                        df = df[df["Ticket Status"].str.lower() == str(args["status"]).lower()]
                    
                    tickets = df.head(50).to_dict(orient="records")
                    print(f"DEBUG: list_tickets found {len(tickets)} rows.")
                    result = json.dumps(tickets, default=str)

                elif func_name == "update_ticket_properties":
                    ticket_id = str(args["ticket_id"])
                    df = get_all_tickets_df()
                    mask = (df["Ticket ID"].astype(str) == ticket_id)
                    
                    if not mask.any():
                        result = "Error: Ticket not found."
                    else:
                        ticket_data = df[mask].iloc[0]
                        can_update = False
                        
                        if self.role == "admin":
                            can_update = True
                        elif self.role == "manager":
                            # Manager can update if ticket is in their team
                            t_team = str(ticket_data.get("Assigned Team", "")).lower()
                            if isinstance(self.team, list):
                                can_update = t_team in [t.lower() for t in self.team]
                            else:
                                can_update = self.team.lower() in t_team
                        elif self.role == "employee":
                            # Employee can only update their own tickets
                            can_update = str(ticket_data.get("User Name", "")).lower() == self.name.lower()

                        if can_update:
                            success = update_multiple_fields(args["ticket_id"], args["updates"])
                            result = "Success" if success else "Failed to update."
                        else:
                            result = "Error: You do not have permission to modify this ticket."

                elif func_name == "get_analytics_report":
                    # Admins get full report if no team specified, others restricted
                    team = args.get("team_name")
                    if not team and self.role != "admin":
                        team = self.team
                    
                    print(f"DEBUG: get_analytics_report for team: {team}")
                    metrics = get_kpi_metrics(team)
                    result = json.dumps(metrics)

                elif func_name == "get_available_resources":
                    # Admins can see resources for any team, others restricted
                    team = args.get("team_name")
                    if not team and self.role != "admin":
                        team = self.team
                        
                    print(f"DEBUG: get_available_resources for team: {team}")
                    resources = get_team_list(team)
                    result = json.dumps(resources)

                elif func_name == "search_invoices":
                    print(f"DEBUG: ChatAgent search_invoices called with: {args}")
                    results = search_invoices(args)
                    result = json.dumps(results, default=str)

                elif func_name == "intelligent_assign_tickets":
                    team = args.get("team_name")
                    if not team and self.role != "admin":
                        team = self.team_str
                    
                    print(f"DEBUG: ChatAgent intelligent_assign_tickets for team: {team}")
                    res = intelligent_assign_tickets(team)
                    result = json.dumps(res)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": func_name,
                    "content": result
                })
        
        return "I encountered an error processing your request.", messages[1:], total_tokens

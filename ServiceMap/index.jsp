
<%@page import="org.disit.servicemap.api.TrafficFlow"%>
<%@page import="org.disit.servicemap.api.ServiceMapApiV1"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ include file= "/include/parameters.jsp" %>
<%
    /* ServiceMap.
   Copyright (C) 2015 DISIT Lab http://www.disit.org - University of Florence

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>. */

    response.setContentType("application/json; charset=UTF-8");
    TrafficFlow tf = new TrafficFlow();
    PrintWriter outt = response.getWriter();

    try{
    
    String geometry = request.getParameter("geometry");
    String dateObserved = request.getParameter("dateObserved");
    String scenarioName = request.getParameter("scenario");
    String roadElement = request.getParameter("roadElement");
    String kind = request.getParameter("kind");


    String isoDateFormatRegex = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}:\\d{2})?$";
    
    
    if (geometry != null && !tf.wktValidator(geometry)) {
        // geometry is not a correct wkt
        String errorMessage = "Invalid geometry format. Please provide the geometry in WKT format.";
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        outt.println(errorMessage);

        ServiceMap.logError(request, response, HttpServletResponse.SC_BAD_REQUEST, errorMessage);
        System.out.println("Invalid geometry format");
        return;
    }

    if (dateObserved != null && !dateObserved.matches(isoDateFormatRegex)) {
        // Date is not in ISO format
        String errorMessage = "Invalid date format. Please provide the date in ISO 8601 format.";
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        outt.println(errorMessage);

        ServiceMap.logError(request, response, HttpServletResponse.SC_BAD_REQUEST, errorMessage);
        System.out.println("Invalid date format");
        return;
    }
    
    if (kind != null && !kind.equals("reconstructed") && !kind.equals("predicted") && !kind.equals("TTT") && !kind.equals("measured")){
        // Only this kind are allowed
        String errorMessage = "Invalid specified. Only 'reconstructed' or 'predicted' or 'TTT' or 'measured'";
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        outt.println(errorMessage);

        ServiceMap.logError(request, response, HttpServletResponse.SC_BAD_REQUEST, errorMessage);
        System.out.println("Invalid specified. Only 'reconstructed' or 'predicted' or 'TTT' or 'measured'");
        return;
    }
    
    
    String resp = tf.trafficFlowSearch(geometry, dateObserved, scenarioName, roadElement, kind);
    outt.println(resp);
    }
    catch(Exception e){
        System.out.println("Error searching TrafficFlow: "+ e);
        ServiceMap.logError(request, response, HttpServletResponse.SC_BAD_REQUEST, "Smething gone wrong");
    }
    

    return;

%>


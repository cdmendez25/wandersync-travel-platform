import { gql } from "@apollo/client";

export const ME = gql`
  query Me {
    me { id email fullName }
  }
`;

export const LOGIN = gql`
  mutation Login($email: String!, $password: String!) {
    login(email: $email, password: $password) { id email fullName }
  }
`;

export const REGISTER = gql`
  mutation Register($email: String!, $password: String!, $fullName: String!) {
    register(email: $email, password: $password, fullName: $fullName) { id email fullName }
  }
`;

export const LOGOUT = gql`
  mutation Logout { logout }
`;

// Asks only for the fields the screen shows (no over-fetching)
export const SEARCH_PACKAGES = gql`
  query SearchPackages($origin: String!, $destination: String!, $date: Date!, $passengers: Int!) {
    searchPackages(origin: $origin, destination: $destination, date: $date, passengers: $passengers) {
      destinationCity
      flights { id airline flightNumber departureAt arrivalAt cabinClass price seatsAvailable }
      hotels { id name stars roomType pricePerNight rating roomsAvailable }
      cars { id company model category transmission pricePerDay unitsAvailable }
    }
  }
`;

const ORDER_FIELDS = `
  id status totalAmount currency checkIn checkOut passengers failureReason createdAt
  payment { status amount providerRef }
  sagaSteps { step action status error createdAt }
`;

export const BOOK_PACKAGE = gql`
  mutation BookPackage($input: BookPackageInput!) {
    bookPackage(input: $input) { ${ORDER_FIELDS} }
  }
`;

export const ORDER = gql`
  query Order($id: ID!) {
    order(id: $id) { ${ORDER_FIELDS} }
  }
`;

export const MY_ORDERS = gql`
  query MyOrders {
    myOrders { id status totalAmount currency checkIn checkOut failureReason createdAt }
  }
`;
